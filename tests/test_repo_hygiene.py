# -*- coding: utf-8 -*-
"""Repository hygiene tests: zero BOMs, zero retired imports, zero hardcoded models/paths, exact active prompts."""
import ast
import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parent.parent
ACTIVE_CODE_DIRS = ("app", "common", "content_pipeline", "parser", "platforms", "x_growth", "tests")

RETIRED_IMPORTS = ()

EXPECTED_ACTIVE_PROMPTS = {
    "prompt/内容生成方案_分析类MLU初筛.txt",
    "prompt/内容生成方案_分析类MLU提取.txt",
    "prompt/内容生成方案_分析类文章生成.txt",
    "prompt/内容生成方案_分析类_发布前总编精选.txt",
    "prompt/内容生成方案_娱乐类_单帖可用性筛选.txt",
    "prompt/内容生成方案_娱乐类_单帖洗稿和神评生成.txt",
    "prompt/内容生成方案_娱乐类_发布前总编精选.txt",
    "prompt/X平台_基于历史表现选帖与改写提示词.txt",
    "prompt/X平台_单篇看图直写提示词.txt",
    "prompt/X平台_结构化自动选帖与改写提示词.txt",
    "prompt/pipeline_v2/post_comment_creation.md",
    "prompt/pipeline_v2/step2_post_comprehension.md",
    "prompt/pipeline_v2/step3_radar_merge.md",
    "prompt/pipeline_v2/step3_topic_radar.md",
    "prompt/pipeline_v2/step4_creation_en_x.md",
    "prompt/pipeline_v2/step4_creation_zh_viral.md",
    "prompt/pipeline_v2/step4_material_curation.md",
    "prompt/pipeline_v2/step5_cold_reader_review.md",
    "prompt/x_growth/auto_reply_comment.md",
    "prompt/x_growth/batch_comment_and_classify.md",
    "prompt/x_growth/batch_score_comments.md",
}


class TestRepoHygiene(unittest.TestCase):
    def _iter_active_py_files(self):
        for folder in ACTIVE_CODE_DIRS:
            d = ROOT / folder
            if d.is_dir():
                for p in sorted(d.rglob("*.py")):
                    if "__pycache__" not in p.parts:
                        yield p

    def test_zero_utf8_boms_in_all_python_files(self):
        bom_files = [
            p.relative_to(ROOT).as_posix()
            for p in sorted(ROOT.rglob("*.py"))
            if not any(part in {".git", ".venv", "__pycache__", ".idea"} for part in p.parts)
            and p.read_bytes().startswith(b"\xef\xbb\xbf")
        ]
        self.assertEqual(bom_files, [], f"UTF-8 BOM found in: {bom_files}")

    def test_zero_retired_imports_and_valid_ast(self):
        for p in self._iter_active_py_files():
            src = p.read_text(encoding="utf-8")
            tree = ast.parse(src, filename=str(p))
            rel = p.relative_to(ROOT)
            if p.name == "test_repo_hygiene.py":
                continue
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        for banned in RETIRED_IMPORTS:
                            self.assertNotIn(banned, alias.name, f"Retired import `{alias.name}` in {rel}")
                elif isinstance(node, ast.ImportFrom):
                    mod = node.module or ""
                    for banned in RETIRED_IMPORTS:
                        self.assertNotIn(banned, mod, f"Retired from-import `{mod}` in {rel}")
            for banned in RETIRED_IMPORTS:
                self.assertIsNone(
                    re.search(rf"\b{re.escape(banned)}\b", src),
                    f"Retired reference `{banned}` found in {rel}",
                )

    def test_no_hardcoded_model_or_personal_drive_paths(self):
        forbidden_literals = (
            "gemini-3.1-pro-preview",
            "E:\\chrome\\",
            "C:\\Users\\zxh\\",
        )
        for folder in ("app", "common", "content_pipeline", "parser", "platforms", "x_growth"):
            for p in sorted((ROOT / folder).rglob("*.py")):
                if "__pycache__" in p.parts:
                    continue
                src = p.read_text(encoding="utf-8")
                for lit in forbidden_literals:
                    self.assertNotIn(lit, src, f"Forbidden literal `{lit}` found in {p.relative_to(ROOT)}")

    def test_expected_active_prompts_exist_and_are_referenced(self):
        prompt_dir = ROOT / "prompt"
        actual = {p.relative_to(ROOT).as_posix() for p in prompt_dir.rglob("*") if p.is_file()}
        self.assertEqual(actual, EXPECTED_ACTIVE_PROMPTS)

        active_corpus = "\n".join(
            p.read_text(encoding="utf-8")
            for folder in ("content_pipeline", "parser", "platforms", "x_growth")
            for p in sorted((ROOT / folder).rglob("*.py"))
        )
        for rel_prompt in EXPECTED_ACTIVE_PROMPTS:
            fname = pathlib.Path(rel_prompt).name
            self.assertIn(fname, active_corpus, f"Prompt `{rel_prompt}` is not referenced by active code")

    def test_platforms_retain_filter_reason_comments(self):
        for name in ("binance_api.py", "okx_api.py", "x_api.py", "x_auto_publisher.py"):
            src = (ROOT / "platforms" / name).read_text(encoding="utf-8")
            self.assertIn("【保留过滤原因说明】", src, f"Missing filter rationale comment in platforms/{name}")


if __name__ == "__main__":
    unittest.main()
