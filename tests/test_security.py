"""
Unit tests for the security scanner.
"""

from __future__ import annotations

import pytest

from src.sandbox.security import SecurityScanner


@pytest.fixture
def scanner():
    return SecurityScanner()


class TestSecurityScanner:
    """Tests for Python code scanning."""

    def test_safe_code_passes(self, scanner):
        code = """
import pandas as pd
import json

df = pd.read_csv('data.csv')
result = df.groupby('category').sum()
print(json.dumps(result.to_dict()))
"""
        result = scanner.scan(code)
        assert result.is_safe

    def test_os_system_detected(self, scanner):
        code = "import os\nos.system('rm -rf /')"
        result = scanner.scan(code)
        assert not result.is_safe
        assert any("os.system" in v for v in result.violations)

    def test_subprocess_detected(self, scanner):
        code = "import subprocess\nsubprocess.run(['ls'])"
        result = scanner.scan(code)
        assert not result.is_safe
        assert any("subprocess" in v for v in result.violations)

    def test_socket_detected(self, scanner):
        code = "import socket\ns = socket.socket()"
        result = scanner.scan(code)
        assert not result.is_safe

    def test_eval_detected(self, scanner):
        code = "result = eval('1 + 1')"
        result = scanner.scan(code)
        assert not result.is_safe
        assert any("eval" in v for v in result.violations)

    def test_exec_detected(self, scanner):
        code = "exec('print(1)')"
        result = scanner.scan(code)
        assert not result.is_safe

    def test_dunder_import_detected(self, scanner):
        code = "mod = __import__('os')"
        result = scanner.scan(code)
        assert not result.is_safe

    def test_requests_import_detected(self, scanner):
        code = "import requests\nr = requests.get('http://evil.com')"
        result = scanner.scan(code)
        assert not result.is_safe

    def test_from_import_detected(self, scanner):
        code = "from subprocess import check_output"
        result = scanner.scan(code)
        assert not result.is_safe

    def test_nested_os_call_detected(self, scanner):
        code = """
import os
path = os.path.join('a', 'b')
os.remove('file.txt')
"""
        result = scanner.scan(code)
        assert not result.is_safe

    def test_syntax_error_flagged(self, scanner):
        code = "def foo(\n  broken syntax"
        result = scanner.scan(code)
        assert not result.is_safe
        assert any("Syntax error" in v for v in result.violations)

    def test_safe_os_path_only(self, scanner):
        """os.path operations should be fine, but os.system should not."""
        # This is a tricky case — os itself isn't banned, but os.system is
        code = """
import os
path = os.path.join('a', 'b')
exists = os.path.exists(path)
"""
        result = scanner.scan(code)
        assert result.is_safe


class TestSQLScanning:
    """Tests for SQL code scanning."""

    def test_safe_sql_passes(self, scanner):
        code = """
SELECT customer_id, SUM(amount) as total
FROM transactions
GROUP BY customer_id
HAVING total > 100
ORDER BY total DESC;
"""
        result = scanner.scan_sql(code)
        assert result.is_safe

    def test_drop_database_detected(self, scanner):
        code = "DROP DATABASE production;"
        result = scanner.scan_sql(code)
        assert not result.is_safe

    def test_truncate_detected(self, scanner):
        code = "TRUNCATE TABLE users;"
        result = scanner.scan_sql(code)
        assert not result.is_safe

    def test_xp_cmdshell_detected(self, scanner):
        code = "EXEC xp_cmdshell 'dir'"
        result = scanner.scan_sql(code)
        assert not result.is_safe

    def test_case_insensitive_detection(self, scanner):
        code = "drop database Production;"
        result = scanner.scan_sql(code)
        assert not result.is_safe
