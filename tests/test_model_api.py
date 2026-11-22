# -*- coding: utf-8 -*-
"""Model gateway unit tests: fast-switch on empty HTTP 200 / throttled 502, cooldown ordering, and lossless-resolution image encoding."""
import base64
import io
import random
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from PIL import Image, ImageChops, ImageDraw

from app.ai_api import model_api


def _make_fake_openai_factory(handler):
    calls = []

    class FakeHttpError(Exception):
        def __init__(self, msg, status_code=None):
            super().__init__(msg)
            self.status_code = status_code

    class FakeCompletions:
        def create(self, **kwargs):
            calls.append(dict(kwargs))
            outcome = handler(kwargs)
            if isinstance(outcome, Exception):
                raise outcome
            model_used, choices_content = outcome
            if choices_content is None:
                choices = []
            else:
                choices = [SimpleNamespace(message=SimpleNamespace(content=choices_content))]
            resp_obj = SimpleNamespace(
                choices=choices,
                model=model_used,
                usage=SimpleNamespace(
                    model_dump=lambda: {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15}
                ),
            )
            return SimpleNamespace(status_code=200, parse=lambda: resp_obj)

    class FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.chat = SimpleNamespace(
                completions=SimpleNamespace(with_raw_response=FakeCompletions())
            )

        def close(self):
            pass

    return FakeClient, FakeHttpError, calls


def _save_noisy_png(path: Path, size, *, mode: str = "RGB") -> int:
    base = Image.effect_noise(size, 16.0)
    grad = Image.linear_gradient("L").resize(size)
    r = ImageChops.blend(base, grad, 0.7)
    g = ImageChops.blend(base, grad.transpose(Image.FLIP_TOP_BOTTOM), 0.7)
    if mode == "RGBA":
        alpha = Image.new("L", size, 255)
        alpha.paste(0, (0, 0, size[0], 60))
        img = Image.merge("RGBA", (r, g, grad, alpha))
    else:
        img = Image.merge("RGB", (r, g, grad))
    img.save(path, format="PNG")
    return path.stat().st_size


class TestModelApi(unittest.TestCase):
    def setUp(self):
        model_api.reset_ai_cooldowns()
        self._tmp = tempfile.TemporaryDirectory()
        self._usage_path = Path(self._tmp.name) / "model_usage.json"

    def tearDown(self):
        model_api.reset_ai_cooldowns()
        self._tmp.cleanup()

    def test_model_lists_remain_intact(self):
        self.assertGreater(len(model_api.HIGH_MODEL_LIST), 0)
        self.assertGreater(len(model_api.MEDIUM_MODEL_LIST), 0)
        self.assertGreater(len(model_api.LOW_MODEL_LIST), 0)
        self.assertTrue(any(m["权重"] > 0 for m in model_api.LOW_MODEL_LIST))

    def test_http_200_empty_choices_and_empty_content_fast_switch_and_cool(self):
        test_models = [
            {"model_name": "empty-model", "权重": 100},
            {"model_name": "healthy-model", "权重": 1},
        ]

        for empty_payload in (None, "   "):
            model_api.reset_ai_cooldowns()

            def _handler(kw, payload=empty_payload):
                if kw["model"] == "empty-model":
                    return (kw["model"], payload)
                return (kw["model"], '{"ok": true}')

            FakeClient, _, calls = _make_fake_openai_factory(_handler)
            with patch.object(model_api, "LOW_MODEL_LIST", test_models), \
                 patch.object(model_api, "API_KEY", "sk-test-key"), \
                 patch.object(model_api, "MODEL_USAGE_JSON_PATH", self._usage_path), \
                 patch.object(model_api, "OpenAI", FakeClient), \
                 patch.object(model_api.time, "sleep", return_value=None), \
                 patch.object(model_api.random, "choices", side_effect=lambda pop, **_: [pop[0]]):
                res1 = model_api.generate_content("hello 1", preset_model_group="low", max_retries_per_model=3)
                self.assertEqual(res1["status"], "✅ 成功")
                self.assertEqual(res1["metrics"]["model_used"], "healthy-model")
                self.assertEqual([c["model"] for c in calls], ["empty-model", "healthy-model"])

                calls.clear()
                ordered = model_api._select_models(None, None, "low")
                self.assertEqual(ordered, ["healthy-model", "empty-model"])

                res2 = model_api.generate_content("hello 2", preset_model_group="low", max_retries_per_model=3)
                self.assertEqual(res2["status"], "✅ 成功")
                self.assertEqual([c["model"] for c in calls], ["healthy-model"])

    def test_http_502_throttled_fast_switch_all_cooling_and_success_reset(self):
        test_models = [
            {"model_name": "throttled-model", "权重": 100},
            {"model_name": "backup-model", "权重": 1},
            {"model_name": "zero-weight-backup", "权重": 0},
        ]
        FakeClient, FakeHttpError, calls = _make_fake_openai_factory(
            lambda kw: (
                FakeHttpError("HTTP 502: 上游429上传上限 throttled", status_code=502)
                if kw["model"] == "throttled-model"
                else (kw["model"], "ok")
            )
        )

        with patch.object(model_api, "LOW_MODEL_LIST", test_models), \
             patch.object(model_api, "API_KEY", "sk-test-key"), \
             patch.object(model_api, "MODEL_USAGE_JSON_PATH", self._usage_path), \
             patch.object(model_api, "OpenAI", FakeClient), \
             patch.object(model_api.time, "sleep", return_value=None), \
             patch.object(model_api.random, "choices", side_effect=lambda pop, **_: [pop[0]]):
            res1 = model_api.generate_content("check 502", preset_model_group="low", max_retries_per_model=3)
            self.assertEqual(res1["status"], "✅ 成功")
            self.assertEqual(calls[0]["model"], "throttled-model")
            self.assertEqual(len([c for c in calls if c["model"] == "throttled-model"]), 1)

            # Throttled model remains in list at tail rather than being removed
            selected = model_api._select_models(None, None, "low")
            self.assertEqual(set(selected), {"throttled-model", "backup-model", "zero-weight-backup"})
            self.assertEqual(selected[-1], "throttled-model")

            # Even when every model cools down, earliest-expiring remains selectable as fallback
            model_api._record_model_outcome("backup-model", success=False, status_code=429, error_text="reset_seconds: 90")
            model_api._record_model_outcome("zero-weight-backup", success=False, status_code=429, error_text="reset_seconds: 180")
            all_cooling = model_api._select_models(None, None, "low")
            self.assertEqual(len(all_cooling), 3)
            self.assertEqual(all_cooling[0], "backup-model")

        # Success clears cooldown immediately
        FakeSuccessClient, _, _ = _make_fake_openai_factory(lambda kw: (kw["model"], "recovered"))
        with patch.object(model_api, "API_KEY", "sk-test-key"), \
             patch.object(model_api, "MODEL_USAGE_JSON_PATH", self._usage_path), \
             patch.object(model_api, "OpenAI", FakeSuccessClient):
            res_rec = model_api.generate_content("recover", model="throttled-model", max_retries_per_model=1)
            self.assertEqual(res_rec["status"], "✅ 成功")
            with model_api._MODEL_COOLDOWN_LOCK:
                self.assertNotIn("throttled-model", model_api._MODEL_COOLDOWN_UNTIL)

    def test_encode_image_for_llm_preserves_dimensions_and_edge_cases(self):
        tmp = Path(self._tmp.name)

        # 1. Tall screenshot PNG -> JPEG Q85 with identical dimensions, smaller size, disk untouched
        tall_path = tmp / "tall_chart.png"
        raw_tall_size = _save_noisy_png(tall_path, (800, 2400), mode="RGB")
        self.assertGreater(raw_tall_size, model_api._IMAGE_COMPRESS_MIN_BYTES)
        orig_bytes = tall_path.read_bytes()

        enc_bytes, enc_mime = model_api._encode_image_for_llm(tall_path, "image/png")
        self.assertEqual(enc_mime, "image/jpeg")
        self.assertLess(len(enc_bytes), raw_tall_size)
        with Image.open(io.BytesIO(enc_bytes)) as decoded:
            self.assertEqual(decoded.size, (800, 2400))
            self.assertEqual(decoded.format, "JPEG")
        self.assertEqual(tall_path.read_bytes(), orig_bytes)

        blocks = model_api._build_content("inspect chart", [str(tall_path)])
        img_block = next(b for b in blocks if b.get("type") == "image_url")
        data_url = img_block["image_url"]["url"]
        self.assertTrue(data_url.startswith("data:image/jpeg;base64,"))
        self.assertEqual(base64.b64decode(data_url.split(",", 1)[1]), enc_bytes)

        # 2. RGBA PNG with transparency -> clean RGB JPEG without exception or black-on-transparent
        rgba_path = tmp / "transparent_chart.png"
        _save_noisy_png(rgba_path, (800, 1200), mode="RGBA")
        rgba_bytes, rgba_mime = model_api._encode_image_for_llm(rgba_path, "image/png")
        self.assertEqual(rgba_mime, "image/jpeg")
        with Image.open(io.BytesIO(rgba_bytes)) as decoded_rgba:
            self.assertEqual(decoded_rgba.size, (800, 1200))
            self.assertEqual(decoded_rgba.mode, "RGB")
            px = decoded_rgba.getpixel((10, 10))
            self.assertTrue(all(ch >= 245 for ch in px))

        # 3. Animated GIF (> 8192 bytes) -> preserved untouched as image/gif
        gif_path = tmp / "anim.gif"
        rng = random.Random(7)
        frames = []
        for f_idx in range(2):
            frame = Image.new("RGB", (256, 256), (255, 255, 255))
            d = ImageDraw.Draw(frame)
            for y in range(0, 256, 4):
                d.line([(0, y), (255, (y + f_idx * 20) % 256)], fill=(rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255)))
            frames.append(frame)
        frames[0].save(gif_path, format="GIF", save_all=True, append_images=frames[1:], duration=100, loop=0)
        gif_raw = gif_path.read_bytes()
        self.assertGreater(len(gif_raw), model_api._IMAGE_COMPRESS_MIN_BYTES)
        gif_out, gif_mime = model_api._encode_image_for_llm(gif_path, "image/gif")
        self.assertEqual(gif_mime, "image/gif")
        self.assertEqual(gif_out, gif_raw)

        # 4. Tiny PNG (<= 8192 bytes) -> preserved untouched
        tiny_path = tmp / "tiny.png"
        Image.new("RGB", (16, 16), (10, 20, 30)).save(tiny_path, format="PNG")
        tiny_raw = tiny_path.read_bytes()
        self.assertLessEqual(len(tiny_raw), model_api._IMAGE_COMPRESS_MIN_BYTES)
        tiny_out, tiny_mime = model_api._encode_image_for_llm(tiny_path, "image/png")
        self.assertEqual(tiny_mime, "image/png")
        self.assertEqual(tiny_out, tiny_raw)

    def test_antigravity_dispatch_and_queue_busy_fast_switch(self):
        tmp = Path(self._tmp.name)
        tiny_path = tmp / "tiny.png"
        Image.new("RGB", (16, 16), (10, 20, 30)).save(tiny_path, format="PNG")

        ag_calls = []

        class FakeAgClient:
            def ask_with_usage(self, **kwargs):
                ag_calls.append(dict(kwargs))
                return "ok-from-ag", {
                    "prompt_tokens": 3,
                    "completion_tokens": 2,
                    "total_tokens": 5,
                    "account_email": "mock-sub@example.com",
                    "sandbox_port": 12345,
                }

        with patch.object(model_api, "get_antigravity_client", return_value=FakeAgClient()), \
             patch.object(model_api, "MODEL_USAGE_JSON_PATH", self._usage_path):
            res_ag = model_api.generate_content(
                "describe image",
                model="antigravity-3.8-flash-high",
                file_paths=[str(tiny_path)],
                max_retries_per_model=1,
            )
            self.assertEqual(res_ag["status"], "✅ 成功")
            self.assertEqual(res_ag["content"], "ok-from-ag")
            self.assertEqual(res_ag["metrics"]["account_used"], "mock-sub@example.com")
            self.assertEqual(len(ag_calls), 1)
            self.assertEqual(len(ag_calls[0]["media"]), 1)
            self.assertEqual(ag_calls[0]["media"][0]["mimeType"], "image/png")

        busy_calls = []

        class BusyAgClient:
            def ask_with_usage(self, **kwargs):
                busy_calls.append(kwargs["model"])
                raise model_api.AntigravityQueueBusyError("busy")

        FakeClient, _, openai_calls = _make_fake_openai_factory(lambda kw: (kw["model"], "fallback-ok"))
        with patch.object(model_api, "get_antigravity_client", return_value=BusyAgClient()), \
             patch.object(model_api, "API_KEY", "sk-test-key"), \
             patch.object(model_api, "MODEL_USAGE_JSON_PATH", self._usage_path), \
             patch.object(model_api, "OpenAI", FakeClient), \
             patch.object(model_api.time, "sleep", return_value=None):
            res_fb = model_api.generate_content(
                "hello fallback",
                model="antigravity-3.8-flash-high",
                fallback_model_list=["antigravity-argon-high", "healthy-model"],
                max_retries_per_model=3,
            )
            self.assertEqual(res_fb["status"], "✅ 成功")
            self.assertEqual(res_fb["content"], "fallback-ok")
            self.assertEqual(busy_calls, ["antigravity-3.8-flash-high"])
            self.assertEqual([c["model"] for c in openai_calls], ["healthy-model"])
            with model_api._MODEL_COOLDOWN_LOCK:
                self.assertNotIn("antigravity-3.8-flash-high", model_api._MODEL_COOLDOWN_UNTIL)


if __name__ == "__main__":
    unittest.main()
