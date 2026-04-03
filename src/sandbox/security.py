"""
Static analysis security scanner for generated code.

All generated code is scanned BEFORE sandbox execution. Code containing
disallowed patterns is rejected with FATAL_ERROR — it never reaches the
sandbox. This is a defense-in-depth layer on top of Docker isolation.
"""

from __future__ import annotations

import ast
import re
from dataclasses import dataclass, field

from src.observability.logger import get_logger

logger = get_logger(__name__)


# Disallowed module imports
DISALLOWED_IMPORTS: set[str] = {
    "subprocess",
    "socket",
    "http",
    "urllib",
    "requests",
    "httpx",
    "aiohttp",
    "ftplib",
    "smtplib",
    "telnetlib",
    "ctypes",
    "multiprocessing",
    "signal",
    "shutil",
}

# Disallowed function calls (module.function patterns)
DISALLOWED_CALLS: set[str] = {
    "os.system",
    "os.popen",
    "os.exec",
    "os.execl",
    "os.execle",
    "os.execlp",
    "os.execv",
    "os.execve",
    "os.execvp",
    "os.execvpe",
    "os.spawn",
    "os.spawnl",
    "os.spawnle",
    "os.kill",
    "os.remove",
    "os.rmdir",
    "os.unlink",
    "os.rename",
}

# Disallowed builtins
DISALLOWED_BUILTINS: set[str] = {
    "eval",
    "exec",
    "compile",
    "__import__",
    "breakpoint",
}

# Regex patterns for additional detection
DISALLOWED_PATTERNS: list[re.Pattern] = [
    re.compile(r"\bos\.system\s*\("),
    re.compile(r"\bsubprocess\b"),
    re.compile(r"\b__import__\s*\("),
    re.compile(r"\beval\s*\("),
    re.compile(r"\bexec\s*\("),
    re.compile(r"\bopen\s*\([^)]*['\"][wa]"),  # open() in write/append mode
]


@dataclass
class ScanResult:
    """Result of a security scan."""

    is_safe: bool = True
    violations: list[str] = field(default_factory=list)

    def add_violation(self, description: str) -> None:
        self.is_safe = False
        self.violations.append(description)


class SecurityScanner:
    """Static analysis scanner for generated Python code.

    Uses AST parsing for precise detection and regex fallback for
    patterns that AST might miss (e.g., dynamic attribute access).
    """

    def scan(self, code: str, filename: str = "<generated>") -> ScanResult:
        """Scan Python code for disallowed patterns.

        Args:
            code: Python source code to scan.
            filename: Source filename for error messages.

        Returns:
            ScanResult with safety verdict and list of violations.
        """
        result = ScanResult()

        # Phase 1: AST-based analysis
        try:
            tree = ast.parse(code, filename=filename)
            self._scan_ast(tree, result, filename)
        except SyntaxError as e:
            result.add_violation(f"Syntax error in {filename}: {e}")
            return result

        # Phase 2: Regex fallback for dynamic patterns
        self._scan_regex(code, result, filename)

        if not result.is_safe:
            logger.warning(
                "Security scan failed",
                extra={
                    "source_file": filename,
                    "violation_count": len(result.violations),
                    "violations": result.violations,
                },
            )
        else:
            logger.info("Security scan passed", extra={"source_file": filename})

        return result

    def scan_sql(self, code: str, filename: str = "<generated.sql>") -> ScanResult:
        """Scan SQL code for dangerous patterns.

        SQL is more limited — we check for obvious injection/escape patterns.
        """
        result = ScanResult()

        dangerous_sql = [
            (r"\bDROP\s+DATABASE\b", "DROP DATABASE statement"),
            (r"\bDROP\s+SCHEMA\b", "DROP SCHEMA statement"),
            (r"\bTRUNCATE\b", "TRUNCATE statement"),
            (r"\bSHUTDOWN\b", "SHUTDOWN command"),
            (r"\bxp_cmdshell\b", "xp_cmdshell usage"),
            (r"\bEXEC\s*\(", "Dynamic SQL execution via EXEC"),
        ]

        for pattern, description in dangerous_sql:
            if re.search(pattern, code, re.IGNORECASE):
                result.add_violation(f"Dangerous SQL in {filename}: {description}")

        return result

    def _scan_ast(self, tree: ast.AST, result: ScanResult, filename: str) -> None:
        """Walk the AST looking for disallowed nodes."""
        for node in ast.walk(tree):
            # Check imports
            if isinstance(node, ast.Import):
                for alias in node.names:
                    module_root = alias.name.split(".")[0]
                    if module_root in DISALLOWED_IMPORTS:
                        result.add_violation(
                            f"Disallowed import '{alias.name}' in {filename} "
                            f"at line {node.lineno}"
                        )

            elif isinstance(node, ast.ImportFrom):
                if node.module:
                    module_root = node.module.split(".")[0]
                    if module_root in DISALLOWED_IMPORTS:
                        result.add_violation(
                            f"Disallowed import from '{node.module}' in {filename} "
                            f"at line {node.lineno}"
                        )

            # Check function calls
            elif isinstance(node, ast.Call):
                call_name = self._get_call_name(node)
                if call_name:
                    if call_name in DISALLOWED_BUILTINS:
                        result.add_violation(
                            f"Disallowed builtin '{call_name}' in {filename} "
                            f"at line {node.lineno}"
                        )
                    elif call_name in DISALLOWED_CALLS:
                        result.add_violation(
                            f"Disallowed call '{call_name}' in {filename} "
                            f"at line {node.lineno}"
                        )

    def _get_call_name(self, node: ast.Call) -> str | None:
        """Extract the fully-qualified name of a function call."""
        if isinstance(node.func, ast.Name):
            return node.func.id
        elif isinstance(node.func, ast.Attribute):
            parts = []
            current = node.func
            while isinstance(current, ast.Attribute):
                parts.append(current.attr)
                current = current.value
            if isinstance(current, ast.Name):
                parts.append(current.id)
                return ".".join(reversed(parts))
        return None

    def _scan_regex(self, code: str, result: ScanResult, filename: str) -> None:
        """Regex-based fallback scan for patterns AST might miss."""
        for pattern in DISALLOWED_PATTERNS:
            matches = pattern.findall(code)
            if matches:
                result.add_violation(
                    f"Suspicious pattern '{pattern.pattern}' found in {filename}"
                )
