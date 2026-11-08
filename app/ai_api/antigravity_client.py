# -*- coding: utf-8 -*-
"""
Antigravity Official Local Client (100% Safe, Zero Reverse-Proxy / Zero Token Extraction)
=========================================================================================

原理说明（为什么绝对安全、不会封号）：
1. 本模块【绝不提取】任何 OAuth Token，也【绝不伪造】任何发往 Google 的请求头或 TLS 指纹。
2. 默认直接通过本地回环地址 (127.0.0.1) 调用您电脑上正在运行的官方 Antigravity 客户端自带的
   `language_server.exe` 原生接口。
3. 纯文本走无状态 `GetModelResponse`；带图片的多模态走 `StartCascade` + `SendUserCascadeMessage`
   并在 `finally` 中立即调用 `DeleteCascadeTrajectory` + `JetboxDeleteSummary` 自动无痕销毁会话。
"""

from __future__ import annotations

import json
import os
import random
import re
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional


# 仅暴露用户指定的两个 High 模式专属模型名（同时兼容原名）
MODEL_ALIASES: Dict[str, str] = {
    "antigravity-argon-high": "MODEL_PLACEHOLDER_M417",
    "gemini-4-argon-high": "MODEL_PLACEHOLDER_M417",
    "antigravity-3.8-flash-high": "MODEL_PLACEHOLDER_M318",
    "gemini-3.8-flash-high": "MODEL_PLACEHOLDER_M318",
}

DEFAULT_LOG_PATH = Path(os.environ.get("APPDATA", "")) / "Antigravity" / "logs" / "language_server.log"
HEADLESS_LOG_PATH = Path(os.environ.get("APPDATA", "")) / "Antigravity" / "logs" / "language_server_headless.log"
DEFAULT_LS_BIN = (
    Path(os.environ.get("LOCALAPPDATA", ""))
    / "Programs"
    / "antigravity"
    / "resources"
    / "bin"
    / "language_server.exe"
)


class AntigravityClient:
    """
    基于官方本地 language_server.exe 的安全调用客户端。
    - 如果 Antigravity 桌面客户端已打开，直接零开销复用其后台进程；
    - 如果 Antigravity 桌面客户端已关闭且 auto_start=True（默认），会自动在后台静默拉起官方
      language_server.exe 独立进程（无窗口、低内存、自动读取 Windows 凭据管理器登录态）；
    - 内置【安全受控并发（Semaphore）+ 错峰发包间隔（Stagger）】，既支持多线程并行提速，
      又确保不会出现超量并发或同一毫秒突发请求。
    """

    def __init__(
        self,
        http_port: Optional[int] = None,
        csrf_token: Optional[str] = None,
        default_model: str = "antigravity-3.8-flash-high",
        timeout: float = 120.0,
        auto_start: bool = True,
        max_concurrency: int = 2,
        min_stagger_seconds: float = 0.8,
        max_stagger_seconds: float = 1.8,
    ) -> None:
        self.default_model = default_model
        self.timeout = timeout
        self.auto_start = auto_start
        self._http_port = http_port
        self._csrf_token = csrf_token

        self._concurrency_sem = threading.BoundedSemaphore(max(1, int(max_concurrency)))
        self._pacing_lock = threading.Lock()
        self._discovery_lock = threading.Lock()
        self._last_dispatch_ts: float = 0.0
        self._min_stagger = max(0.0, float(min_stagger_seconds))
        self._max_stagger = max(self._min_stagger, float(max_stagger_seconds))

    @staticmethod
    def _get_live_ls_processes() -> Dict[int, str]:
        """获取当前正在运行的 language_server.exe 进程列表: {pid: csrf_token}。"""
        live: Dict[int, str] = {}
        try:
            cmd = [
                "powershell",
                "-NoProfile",
                "-Command",
                "Get-CimInstance Win32_Process -Filter \"Name='language_server.exe'\" "
                "| Select-Object ProcessId, CommandLine | ConvertTo-Json -Compress",
            ]
            out = subprocess.check_output(cmd, text=True, timeout=10, stderr=subprocess.DEVNULL).strip()
            if not out:
                return live
            data = json.loads(out)
            items = data if isinstance(data, list) else [data]
            for item in items:
                pid = item.get("ProcessId")
                cmdline = item.get("CommandLine") or ""
                m_token = re.search(r"--csrf_token\s+([0-9a-fA-F\-]+)", cmdline)
                if pid and m_token:
                    live[int(pid)] = m_token.group(1)
        except Exception:
            pass
        return live

    @staticmethod
    def _parse_log_pid_and_port(log_path: Path) -> tuple[Optional[int], Optional[int]]:
        """从指定日志文件中解析 language_server 的 PID 和 HTTP 端口。"""
        if not log_path.exists():
            return None, None
        try:
            with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                head = f.read(65536)
            m_pid = re.search(r"Starting language server process with pid (\d+)", head)
            m_port = re.search(r"Language server listening on random port at (\d+) for HTTP\b", head)
            pid = int(m_pid.group(1)) if m_pid else None
            port = int(m_port.group(1)) if m_port else None
            return pid, port
        except OSError:
            return None, None

    @staticmethod
    def _detect_ide_version() -> str:
        """从官方日志中动态读取当前 language_server.exe 的真实版本号。"""
        for log_path in (DEFAULT_LOG_PATH, HEADLESS_LOG_PATH):
            if log_path.exists():
                try:
                    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
                        head = f.read(8192)
                    m = re.search(r"Language server version:\s*([0-9]+\.[0-9]+\.[0-9]+)", head)
                    if m:
                        return m.group(1)
                except OSError:
                    pass
        return "2.16.0"

    def _start_headless_server(self) -> tuple[int, str]:
        """在桌面客户端关闭的情况下，后台静默启动官方 language_server.exe（无 GUI 窗口）。"""
        if not DEFAULT_LS_BIN.exists():
            raise RuntimeError(f"未找到官方 language_server.exe: {DEFAULT_LS_BIN}")

        ide_version = self._detect_ide_version()
        csrf_token = str(uuid.uuid4())
        HEADLESS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(HEADLESS_LOG_PATH, "w", encoding="utf-8")

        args = [
            str(DEFAULT_LS_BIN),
            "--standalone",
            "--override_ide_name", "antigravity",
            "--subclient_type", "hub",
            "--override_ide_version", ide_version,
            "--override_user_agent_name", "antigravity",
            "--https_server_port", "0",
            "--csrf_token", csrf_token,
            "--app_data_dir", "antigravity",
            "--api_server_url", "https://generativelanguage.googleapis.com",
            "--cloud_code_endpoint", "https://daily-cloudcode-pa.googleapis.com",
            "--enable_sidecars",
        ]

        creationflags = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000) | getattr(
            subprocess, "DETACHED_PROCESS", 0x00000008
        )
        subprocess.Popen(
            args,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
            close_fds=True,
        )

        http_port: Optional[int] = None
        deadline = time.time() + 15.0
        while time.time() < deadline:
            try:
                content = HEADLESS_LOG_PATH.read_text(encoding="utf-8", errors="ignore")
                if not http_port:
                    m = re.search(r"Language server listening on random port at (\d+) for HTTP\b", content)
                    if m:
                        http_port = int(m.group(1))
                if http_port and ("Auth succeeded" in content or "initialized server successfully" in content):
                    self._http_port = http_port
                    self._csrf_token = csrf_token
                    return http_port, csrf_token
            except OSError:
                pass
            time.sleep(0.25)

        if http_port:
            self._http_port = http_port
            self._csrf_token = csrf_token
            return http_port, csrf_token

        raise RuntimeError("后台静默启动官方 language_server.exe 超时，请检查网络连接或日志。")

    def _discover_local_server(self) -> tuple[int, str]:
        """自动发现正在运行的官方 language_server.exe；若客户端已关闭且允许自启，则自动后台静默拉起。"""
        with self._discovery_lock:
            if self._http_port and self._csrf_token:
                return self._http_port, self._csrf_token

            live_procs = self._get_live_ls_processes()

            for log_path in (DEFAULT_LOG_PATH, HEADLESS_LOG_PATH):
                pid, port = self._parse_log_pid_and_port(log_path)
                if pid and port and pid in live_procs:
                    self._http_port = port
                    self._csrf_token = live_procs[pid]
                    return self._http_port, self._csrf_token

            if self.auto_start:
                return self._start_headless_server()

            raise RuntimeError(
                "未能自动检测到正在运行的 Antigravity (language_server.exe)，且 auto_start=False。"
            )

    def _ensure_connection_info(self) -> tuple[int, str]:
        if self._http_port and self._csrf_token:
            return self._http_port, self._csrf_token
        return self._discover_local_server()

    def _wait_for_stagger_slot(self) -> None:
        """确保每次大模型请求的发起时间错开 0.8~1.8 秒，消除『同一毫秒并发轰炸』的脚本特征。"""
        if self._max_stagger <= 0:
            return
        with self._pacing_lock:
            now = time.monotonic()
            target_gap = random.uniform(self._min_stagger, self._max_stagger)
            wait_sec = (self._last_dispatch_ts + target_gap) - now
            if wait_sec > 0:
                time.sleep(wait_sec)
            self._last_dispatch_ts = time.monotonic()

    def _post_rpc(
        self,
        method: str,
        payload: Dict[str, Any],
        retry_discovery: bool = True,
        apply_rate_limit: bool = False,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """调用本地官方 LanguageServerService ConnectRPC 接口（强制直连 127.0.0.1，不经过外部代理）。"""
        if apply_rate_limit:
            with self._concurrency_sem:
                self._wait_for_stagger_slot()
                return self._post_rpc_inner(method, payload, retry_discovery=retry_discovery, timeout=timeout)
        return self._post_rpc_inner(method, payload, retry_discovery=retry_discovery, timeout=timeout)

    def _post_rpc_inner(
        self,
        method: str,
        payload: Dict[str, Any],
        retry_discovery: bool = True,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        port, token = self._ensure_connection_info()
        url = f"http://127.0.0.1:{port}/exa.language_server_pb.LanguageServerService/{method}"
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            url,
            data=body,
            headers={
                "Content-Type": "application/json",
                "x-codeium-csrf-token": token,
            },
            method="POST",
        )
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        req_timeout = float(timeout) if timeout is not None else self.timeout
        try:
            with opener.open(req, timeout=req_timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            err_body = ""
            try:
                err_body = exc.read().decode("utf-8", errors="ignore")
            except Exception:
                pass
            err = RuntimeError(
                f"Antigravity 服务端返回 HTTP {exc.code} ({method}): {err_body or exc.reason}"
            )
            setattr(err, "status_code", exc.code)
            raise err from exc
        except (urllib.error.URLError, ConnectionError) as exc:
            if retry_discovery:
                with self._discovery_lock:
                    self._http_port = None
                    self._csrf_token = None
                return self._post_rpc_inner(method, payload, retry_discovery=False, timeout=timeout)
            raise RuntimeError(f"调用本地 Antigravity LanguageServer ({method}) 网络连接失败: {exc}") from exc

    @staticmethod
    def resolve_model_enum(model: str) -> str:
        """将模型名称转换为内部枚举名（仅允许指定的两个 High 模型或枚举）。"""
        if model.startswith("MODEL_"):
            return model
        if model in MODEL_ALIASES:
            return MODEL_ALIASES[model]
        raise ValueError(
            f"不支持的 Antigravity 本地模型名: {model}。"
            f"可用模型名: {list(MODEL_ALIASES.keys())}"
        )

    def ask_with_usage(
        self,
        prompt: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        media: Optional[List[Dict[str, str]]] = None,
        timeout: Optional[float] = None,
    ) -> tuple[str, Optional[Dict[str, int]]]:
        """
        统一的模型生成接口（同时支持纯文本与多张图片分析，返回 (reply_text, usage_dict)）。
        - 无图片时：走极速无状态 GetModelResponse 接口；
        - 有图片时：走 StartCascade -> SendUserCascadeMessage (含 media) -> GetCascadeTrajectory，
          并在 finally 中调用 DeleteCascadeTrajectory + JetboxDeleteSummary 彻底无痕销毁临时会话。
        """
        target_model = self.resolve_model_enum(model or self.default_model)
        full_prompt = f"{system_prompt.strip()}\n\n{prompt}" if system_prompt else prompt
        req_timeout = float(timeout) if timeout is not None else self.timeout

        # 通道 A：无图片附件 -> 走无状态 GetModelResponse
        if not media:
            resp = self._post_rpc(
                "GetModelResponse",
                {"prompt": full_prompt, "model": target_model},
                apply_rate_limit=True,
                timeout=req_timeout,
            )
            return resp.get("response", ""), None

        # 通道 B：含图片附件 -> 走多模态 Cascade 通道并自动无痕清理
        with self._concurrency_sem:
            self._wait_for_stagger_slot()
            start_resp = self._post_rpc_inner(
                "StartCascade",
                {
                    "source": "CORTEX_TRAJECTORY_SOURCE_CASCADE_CLIENT",
                    "trajectoryType": "CORTEX_TRAJECTORY_TYPE_CASCADE",
                    "requestedModel": target_model,
                },
                timeout=min(req_timeout, 30.0),
            )
            cascade_id = start_resp.get("cascadeId")
            if not cascade_id:
                raise RuntimeError("StartCascade 未返回有效的 cascadeId")

            try:
                no_tool_guard = (
                    "【系统强制约束：当前为非交互式多模态分析模式，严禁调用任何工具(tools/subagents/commands)，"
                    "请直接阅读上传的图片与下方提示词，直接输出最终文本或JSON结果。】\n\n"
                )
                self._post_rpc_inner(
                    "SendUserCascadeMessage",
                    {
                        "cascadeId": cascade_id,
                        "items": [{"text": no_tool_guard + full_prompt}],
                        "media": media,
                        "cascadeConfig": {
                            "plannerConfig": {
                                "conversational": {"agenticMode": False},
                                "requestedModel": {"model": target_model},
                                "planModel": target_model,
                            }
                        },
                    },
                    timeout=req_timeout,
                )
                wait_resp = self._post_rpc_inner(
                    "WaitForConversationFullyIdle",
                    {
                        "conversationId": cascade_id,
                        "inactivityTimeoutSeconds": max(15, int(req_timeout)),
                        "returnOnExecutorError": True,
                    },
                    timeout=req_timeout + 10.0,
                )
                if wait_resp.get("timedOut"):
                    raise TimeoutError(f"Antigravity 多模态图片分析超时 ({req_timeout}s)")

                traj_resp = self._post_rpc_inner(
                    "GetCascadeTrajectory",
                    {"cascadeId": cascade_id},
                    timeout=30.0,
                )
                steps = traj_resp.get("trajectory", {}).get("steps", [])
                planner_steps = [
                    s for s in steps
                    if s.get("type") == "CORTEX_STEP_TYPE_PLANNER_RESPONSE"
                    and (s.get("plannerResponse") or {}).get("response")
                ]
                if not planner_steps:
                    raise RuntimeError("Antigravity 多模态调用未返回有效的 plannerResponse 文本")

                last_step = planner_steps[-1]
                reply_text = (last_step.get("plannerResponse") or {}).get("response", "")
                model_usage = (last_step.get("metadata") or {}).get("modelUsage") or {}
                usage_dict: Optional[Dict[str, int]] = None
                if model_usage:
                    p_tok = int(model_usage.get("inputTokens") or 0)
                    c_tok = int(model_usage.get("outputTokens") or 0)
                    usage_dict = {
                        "prompt_tokens": p_tok,
                        "completion_tokens": c_tok,
                        "total_tokens": p_tok + c_tok,
                    }
                return reply_text, usage_dict
            finally:
                # 无论成功或异常，立即无痕删除临时会话与桌面端摘要记录
                for cleanup_method in ("DeleteCascadeTrajectory", "JetboxDeleteSummary"):
                    try:
                        self._post_rpc_inner(
                            cleanup_method,
                            {"cascadeId": cascade_id},
                            retry_discovery=False,
                            timeout=10.0,
                        )
                    except Exception:
                        pass

    def ask(
        self,
        prompt: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        media: Optional[List[Dict[str, str]]] = None,
        timeout: Optional[float] = None,
    ) -> str:
        reply, _ = self.ask_with_usage(
            prompt=prompt,
            model=model,
            system_prompt=system_prompt,
            media=media,
            timeout=timeout,
        )
        return reply

    def ask_json(
        self,
        prompt: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
        media: Optional[List[Dict[str, str]]] = None,
        timeout: Optional[float] = None,
    ) -> Dict[str, Any]:
        """要求模型返回 JSON 格式并自动解析为 Python 字典。"""
        json_instruction = (
            (system_prompt or "")
            + "\n请严格只输出合法的 JSON 对象，不要包含 ```json 代码块包裹或任何额外解释文字。"
        ).strip()
        raw = self.ask(
            prompt=prompt,
            model=model,
            system_prompt=json_instruction,
            media=media,
            timeout=timeout,
        ).strip()
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
        return json.loads(raw)

    def get_user_status(self) -> Dict[str, Any]:
        """获取当前登录账号信息、会员等级 (Google AI Ultra) 以及各模型剩余额度比例。"""
        return self._post_rpc("GetUserStatus", {})

    def list_models(self) -> List[Dict[str, Any]]:
        """列出当前账号可用的所有模型及其剩余额度百分比与重置时间。"""
        status = self.get_user_status()
        configs = (
            status.get("userStatus", {})
            .get("cascadeModelConfigData", {})
            .get("clientModelConfigs", [])
        )
        result = []
        for cfg in configs:
            quota = cfg.get("quotaInfo", {})
            result.append(
                {
                    "label": cfg.get("label"),
                    "model_id": cfg.get("modelId"),
                    "model_enum": cfg.get("modelOrAlias", {}).get("model"),
                    "remaining_fraction": quota.get("remainingFraction"),
                    "reset_time": quota.get("resetTime"),
                }
            )
        return result


_default_client_lock = threading.Lock()
_default_client: Optional[AntigravityClient] = None


def get_client(max_concurrency: int = 2) -> AntigravityClient:
    """获取进程级全局共享的 AntigravityClient 单例。"""
    global _default_client
    if _default_client is None:
        with _default_client_lock:
            if _default_client is None:
                _default_client = AntigravityClient(max_concurrency=max_concurrency)
    return _default_client


def ask_antigravity(
    prompt: str,
    model: str = "antigravity-3.8-flash-high",
    system_prompt: Optional[str] = None,
) -> str:
    return get_client().ask(prompt=prompt, model=model, system_prompt=system_prompt)


def ask_antigravity_json(
    prompt: str,
    model: str = "antigravity-3.8-flash-high",
    system_prompt: Optional[str] = None,
) -> Dict[str, Any]:
    return get_client().ask_json(prompt=prompt, model=model, system_prompt=system_prompt)


ask = ask_antigravity
ask_json = ask_antigravity_json
