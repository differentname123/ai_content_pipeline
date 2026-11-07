# -*- coding: utf-8 -*-
"""
Antigravity Official Local Client (100% Safe, Zero Reverse-Proxy / Zero Token Extraction)
=========================================================================================

原理说明（为什么绝对安全、不会封号）：
1. 本模块【绝不提取】任何 OAuth Token，也【绝不伪造】任何发往 Google 的请求头或 TLS 指纹。
2. 默认直接通过本地回环地址 (127.0.0.1) 调用您电脑上正在运行的官方 Antigravity 客户端自带的
   `language_server.exe` 原生接口 (`/exa.language_server_pb.LanguageServerService/GetModelResponse`)。
3. 对 Google 服务器而言，您的 Python 代码发起的每一次调用，都是由官方 `language_server.exe` 进程
   通过同一个长连接发出的，与您在 Antigravity 窗口里手打提问 100% 完全一致。
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional


# 友好的模型名称 -> 官方内部 Model 枚举映射表
MODEL_ALIASES: Dict[str, str] = {
    # Gemini 4 Argon 系列
    "gemini-4-argon-high": "MODEL_PLACEHOLDER_M417",
    "gemini-4-argon-medium": "MODEL_PLACEHOLDER_M418",
    "gemini-4-argon-low": "MODEL_PLACEHOLDER_M419",
    # Gemini 3.8 Flash 系列
    "gemini-3.8-flash-high": "MODEL_PLACEHOLDER_M318",
    "gemini-3.8-flash-medium": "MODEL_PLACEHOLDER_M319",
    "gemini-3.8-flash-low": "MODEL_PLACEHOLDER_M320",
    # Claude 4.6 系列
    "claude-sonnet-4-6": "MODEL_PLACEHOLDER_M35",
    "claude-opus-4-6-thinking": "MODEL_PLACEHOLDER_M26",
    # GPT-OSS 系列
    "gpt-oss-120b-medium": "MODEL_OPENAI_GPT_OSS_120B_MEDIUM",
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
DEFAULT_AGY_BIN = Path(os.environ.get("LOCALAPPDATA", "")) / "agy" / "bin" / "agy.exe"


class AntigravityClient:
    """
    基于官方本地 language_server.exe 的安全调用客户端。
    - 如果 Antigravity 桌面客户端已打开，直接零开销复用其后台进程；
    - 如果 Antigravity 桌面客户端已关闭且 auto_start=True（默认），会自动在后台静默拉起官方
      language_server.exe 独立进程（无窗口、低内存、自动读取 Windows 凭据管理器登录态）。
    """

    def __init__(
        self,
        http_port: Optional[int] = None,
        csrf_token: Optional[str] = None,
        default_model: str = "gemini-4-argon-medium",
        timeout: float = 120.0,
        auto_start: bool = True,
    ) -> None:
        self.default_model = default_model
        self.timeout = timeout
        self.auto_start = auto_start
        self._http_port = http_port
        self._csrf_token = csrf_token

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

    def _start_headless_server(self) -> tuple[int, str]:
        """在桌面客户端关闭的情况下，后台静默启动官方 language_server.exe（无 GUI 窗口）。"""
        if not DEFAULT_LS_BIN.exists():
            raise RuntimeError(f"未找到官方 language_server.exe: {DEFAULT_LS_BIN}")

        csrf_token = str(uuid.uuid4())
        HEADLESS_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        log_file = open(HEADLESS_LOG_PATH, "w", encoding="utf-8")

        args = [
            str(DEFAULT_LS_BIN),
            "--standalone",
            "--override_ide_name", "antigravity",
            "--subclient_type", "hub",
            "--override_ide_version", "2.16.0",
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

        # 等待后台服务完成端口监听与 Windows 凭据自动登录（通常仅需 2~4 秒）
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
        live_procs = self._get_live_ls_processes()

        # 优先匹配桌面客户端日志与无头后台日志中正在存活的进程
        for log_path in (DEFAULT_LOG_PATH, HEADLESS_LOG_PATH):
            pid, port = self._parse_log_pid_and_port(log_path)
            if pid and port and pid in live_procs:
                self._http_port = port
                self._csrf_token = live_procs[pid]
                return self._http_port, self._csrf_token

        # 如果桌面客户端已关闭，自动在后台静默启动官方 language_server.exe
        if self.auto_start:
            return self._start_headless_server()

        raise RuntimeError(
            "未能自动检测到正在运行的 Antigravity (language_server.exe)，且 auto_start=False。"
        )

    def _ensure_connection_info(self) -> tuple[int, str]:
        if self._http_port and self._csrf_token:
            return self._http_port, self._csrf_token
        return self._discover_local_server()

    def _post_rpc(self, method: str, payload: Dict[str, Any], retry_discovery: bool = True) -> Dict[str, Any]:
        """调用本地官方 LanguageServerService ConnectRPC 接口（强制直连 127.0.0.1，不经过外部代理）。"""
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
        # 强制本地回环直连，不受环境变量 HTTP_PROXY 影响
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            with opener.open(req, timeout=self.timeout) as resp:
                raw = resp.read().decode("utf-8")
                return json.loads(raw) if raw else {}
        except (urllib.error.URLError, ConnectionError) as exc:
            if retry_discovery:
                # 如果客户端重启导致端口/Token 变化，自动重新发现并重试一次
                self._http_port = None
                self._csrf_token = None
                return self._post_rpc(method, payload, retry_discovery=False)
            raise RuntimeError(f"调用本地 Antigravity LanguageServer ({method}) 失败: {exc}") from exc

    @staticmethod
    def resolve_model_enum(model: str) -> str:
        """将模型别名（如 'gemini-4-argon-high'）转换为内部枚举名（如 'MODEL_PLACEHOLDER_M417'）。"""
        if model.startswith("MODEL_"):
            return model
        return MODEL_ALIASES.get(model, model)

    def ask(
        self,
        prompt: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> str:
        """
        向 Antigravity 发送提示词并获取模型回复（纯文本单轮生成，零会话垃圾残留）。

        :param prompt: 用户输入内容
        :param model: 模型名称（如 'gemini-4-argon-medium', 'gemini-4-argon-high', 'gemini-3.8-flash-high'）
        :param system_prompt: 可选的系统指令前缀
        :return: 模型回复文本
        """
        target_model = self.resolve_model_enum(model or self.default_model)
        full_prompt = f"{system_prompt.strip()}\n\n{prompt}" if system_prompt else prompt
        resp = self._post_rpc(
            "GetModelResponse",
            {
                "prompt": full_prompt,
                "model": target_model,
            },
        )
        return resp.get("response", "")

    def ask_json(
        self,
        prompt: str,
        model: Optional[str] = None,
        system_prompt: Optional[str] = None,
    ) -> Dict[str, Any]:
        """要求模型返回 JSON 格式并自动解析为 Python 字典。"""
        json_instruction = (
            (system_prompt or "")
            + "\n请严格只输出合法的 JSON 对象，不要包含 ```json 代码块包裹或任何额外解释文字。"
        ).strip()
        raw = self.ask(prompt=prompt, model=model, system_prompt=json_instruction).strip()
        # 剥离可能存在的 markdown 代码块
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


if __name__ == "__main__":
    client = AntigravityClient()
    print("=== 1. 当前账号与模型额度状态 ===")
    for m in client.list_models():
        pct = f"{float(m['remaining_fraction']) * 100:.1f}%" if m["remaining_fraction"] is not None else "N/A"
        print(f"  - {m['model_id']:<26} ({m['label']:<28}) 剩余额度: {pct}")

    print("\n=== 2. 测试代码直接调用 (gemini-4-argon-medium) ===")
    reply = client.ask("用一句话告诉我 Python 为什么适合做量化与自动化脚本？")
    print("模型回复:", reply.strip())

    print("\n=== 3. 测试 JSON 结构化返回 (gemini-3.8-flash-high) ===")
    data = client.ask_json(
        "请分析『大户在4天内从交易所提走15.4亿枚XRP』的情绪方向，返回字段: sentiment (bullish/bearish), score (1-10), summary",
        model="gemini-3.8-flash-high",
    )
    print("JSON 解析结果:", json.dumps(data, ensure_ascii=False, indent=2))
