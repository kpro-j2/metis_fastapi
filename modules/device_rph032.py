import os
import socket
from typing import Dict, List, Optional


class RPH032:
    def __init__(self, ip: Optional[str] = None, port: Optional[int] = None, timeout: float = 1.0):
        self.ip = ip or os.environ.get("HV_IP", "192.168.10.16")
        self.port = int(port or os.environ.get("HV_PORT", "4660"))
        self.timeout = timeout

    @staticmethod
    def _check_ch(ch: int) -> None:
        if ch < 1 or ch > 4:
            raise ValueError("ch must be 1..4")

    def _rbcp_write(self, addr: int, data: List[int]) -> None:
        pkt = bytes([0xFF, 0x80, 0x00, len(data), 0x00, 0x00, 0x00, addr]) + bytes(data)

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(self.timeout)
        try:
            s.sendto(pkt, (self.ip, self.port))
            try:
                s.recvfrom(1024)
            except TimeoutError:
                pass
        finally:
            s.close()

    def _rbcp_read16(self, addr: int) -> int:
        pkt = bytes([0xFF, 0xC0, 0x00, 0x02, 0x00, 0x00, 0x00, addr])

        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.settimeout(self.timeout)
        try:
            s.sendto(pkt, (self.ip, self.port))
            data, _ = s.recvfrom(1024)
        finally:
            s.close()

        if len(data) < 10:
            raise RuntimeError("RBCP read error: response is too short")

        return data[8] * 256 + data[9]

    def _write8(self, addr: int, val: int) -> None:
        self._rbcp_write(addr, [val & 0xFF])

    def _write16(self, addr: int, val: int) -> None:
        self._rbcp_write(addr, [(val >> 8) & 0xFF, val & 0xFF])

    def remote_on(self) -> None:
        self._write8(0x05, 1)

    def remote_off(self) -> None:
        self._write8(0x05, 0)

    def kill_off(self) -> None:
        self._write8(0x0B, 0)

    def kill_all(self) -> None:
        self._write8(0x0B, 0x0F)

    def set_voltage(self, ch: int, volt: int) -> None:
        self._check_ch(ch)
        self._write16(0x10 + 2 * (ch - 1), volt)

    def read_set_voltage(self, ch: int) -> int:
        self._check_ch(ch)
        return self._rbcp_read16(0x10 + 2 * (ch - 1))

    def set_current_limit(self, ch: int, limit: int) -> None:
        self._check_ch(ch)
        self._write16(0x20 + 2 * (ch - 1), limit)

    def read_current_limit(self, ch: int) -> int:
        self._check_ch(ch)
        return self._rbcp_read16(0x20 + 2 * (ch - 1))

    def set_ramp(self, ch: int, ramp: int) -> None:
        self._check_ch(ch)
        self._write8(0x0C + (ch - 1), ramp)

    def read_ramp(self, ch: int) -> int:
        self._check_ch(ch)
        return self._rbcp_read16(0x0C + (ch - 1)) & 0xFF

    def read_voltage(self, ch: int) -> int:
        self._check_ch(ch)
        return self._rbcp_read16(0x18 + 2 * (ch - 1))

    def read_current(self, ch: int) -> int:
        self._check_ch(ch)
        return self._rbcp_read16(0x30 + 2 * (ch - 1))

    def status(self, ch: int) -> Dict[str, int]:
        self._check_ch(ch)
        return {
            "ch": ch,
            "voltage": self.read_voltage(ch),
            "current": self.read_current(ch),
        }

    def safe_on(self, ch: int, volt: int, limit: int = 100, ramp: int = 10) -> None:
        self.remote_on()
        self.set_current_limit(ch, limit)
        self.set_ramp(ch, ramp)
        self.kill_off()
        self.set_voltage(ch, volt)

    def safe_off(self, ch: int) -> None:
        self.set_voltage(ch, 0)
