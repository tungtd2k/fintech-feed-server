from __future__ import annotations

# Tham số CLI điều khiển tốc độ replay; asyncio chạy đồng thời replay và
# heartbeat; csv/json phục vụ protocol file và WebSocket.
import argparse
import asyncio
import csv
import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import websockets
from websockets.asyncio.server import ServerConnection

@dataclass(frozen=True)
class Tick:
    """Một dòng OHLCV chuẩn hóa có thể gửi qua protocol replay."""

    # Mã xác định subscription nào sẽ nhận được dòng dữ liệu.
    symbol: str
    # Timestamp giữ dạng chuỗi ISO cho tới khi Flutter map sang DateTime.
    timestamp: str
    # OHLC giữ dạng float để payload JSON đơn giản và dễ dùng trên nhiều nền tảng.
    open: float
    high: float
    low: float
    price: float
    volume: int

def load_ticks(folder: Path) -> list[Tick]:
    """Đọc mọi dòng CSV, chuẩn hóa rồi sắp xếp toàn bộ feed theo thời gian."""

    # Dùng một list để tick của mọi mã được replay trên cùng một đồng hồ.
    ticks = []
    # Mỗi file CSV đại diện cho một mã và có cùng các cột OHLCV.
    for path in folder.glob("*.csv"):
        # newline='' giúp csv.DictReader xử lý line ending nhất quán.
        with path.open(encoding="utf-8", newline="") as file:
            # DictReader map mỗi dòng vào các cột có tên trong CSV.
            for row in csv.DictReader(file):
                # close là giá replay chính; OHLC thiếu sẽ lấy close để dữ liệu
                # sinh/demo vẫn hợp lệ.
                close = float(row["close"])
                ticks.append(Tick(row.get("symbol", path.stem).upper(), row["timestamp"], float(row.get("open", close)), float(row.get("high", close)), float(row.get("low", close)), close, int(row["volume"])))
    # Vòng replay phụ thuộc vào thứ tự thời gian của toàn bộ mã.
    return sorted(ticks, key=lambda tick: datetime.fromisoformat(tick.timestamp))

class Replay:
    def __init__(self, ticks: list[Tick], speed: float) -> None:
        # Giữ dữ liệu nguồn bất biến và các điều khiển replay có thể thay đổi.
        self.ticks, self.speed, self.seq = ticks, speed, 0
        # Mỗi client kết nối được map với các mã đang subscribe.
        self.clients: dict[ServerConnection, set[str]] = {}
        # latest dùng để gửi snapshot ngay khi có subscription mới.
        self.latest: dict[str, Tick] = {}
        # ticks đã theo thứ tự thời gian, nên dòng cuối của mỗi mã là giá mới
        # nhất. Duyệt xuôi để không vô tình chọn dòng cũ nhất.
        for tick in ticks:
            self.latest[tick.symbol] = tick
        # Một lock bảo vệ subscription khi replay/heartbeat đang duyệt client.
        self.lock = asyncio.Lock()

    async def send(self, client: ServerConnection, payload: dict) -> None:
        # Serialize một message protocol và gửi tới đúng một client.
        await client.send(json.dumps(payload))

    async def handler(self, client: ServerConnection) -> None:
        # Kết nối mới bắt đầu chưa có mã nào cho tới khi gửi subscribe.
        async with self.lock:
            self.clients[client] = set()
        try:
            # async-for chờ command từ client này cho tới khi client ngắt kết nối.
            async for raw in client:
                # Mỗi command từ Flutter là JSON có trường type.
                message = json.loads(raw)
                if message.get("type") == "subscribe":
                    # Chuẩn hóa mã thành chữ hoa và loại mã trùng.
                    symbols = {str(symbol).upper() for symbol in message.get("symbols", [])}
                    async with self.lock:
                        # Ghi đè set giúp resubscribe/reconnect không tạo dữ liệu trùng.
                        self.clients[client] = symbols
                    for symbol in symbols:
                        # Gửi 120 dòng mới nhất, không lấy 120 dòng đầu file.
                        symbol_ticks = [tick for tick in self.ticks if tick.symbol == symbol]
                        history = symbol_ticks[-120:]
                        # Gửi history trước để chart vẽ được trước khi nhận tick mới.
                        await self.send(client, {"type": "history", "ticks": [tick.__dict__ for tick in history]})
                        if tick := self.latest.get(symbol):
                            # Snapshot cung cấp điểm hiện tại sau history và có seq
                            # tăng dần.
                            self.seq += 1
                            await self.send(client, {"type": "snapshot", "seq": self.seq, **tick.__dict__})
                elif message.get("type") == "set_speed":
                    # Giới hạn speed để giá trị lỗi/0 không gây chia cho 0.
                    self.speed = max(0.1, float(message.get("speed", 1)))
                elif message.get("type") == "heartbeat":
                    # Heartbeat xác nhận socket vẫn đang phản hồi.
                    await self.send(client, {"type": "heartbeat", "timestamp": datetime.now().isoformat()})
                elif message.get("type") == "disconnect":
                    # Command này chủ động kích hoạt logic reconnect của Flutter.
                    await client.close(code=4000, reason="intentional disconnect")
                    break
        except websockets.ConnectionClosed:
            # Client đóng bình thường không phải lỗi server; Flutter sẽ reconnect.
            pass
        finally:
            # Luôn xóa client kể cả khi parse hoặc gửi dữ liệu bị lỗi.
            async with self.lock:
                self.clients.pop(client, None)

    async def replay(self) -> None:
        """Replay các dòng theo thời gian và phát tới các subscriber."""

        previous = None
        for tick in self.ticks:
            # Chuyển khoảng cách thời gian nguồn thành độ trễ demo có giới hạn.
            current = datetime.fromisoformat(tick.timestamp)
            if previous:
                await asyncio.sleep(min(max((current - previous).total_seconds() / max(self.speed, 0.1), 0.01), 2))
            previous = current
            # Cập nhật giá mới nhất cho client subscribe giữa chừng replay.
            self.latest[tick.symbol] = tick
            async with self.lock:
                # Chỉ gửi tick tới client đã đăng ký mã tương ứng.
                clients = [client for client, symbols in self.clients.items() if tick.symbol in symbols]
            for client in clients:
                # seq giúp Flutter loại tick cũ hoặc tick đến sai thứ tự.
                self.seq += 1
                try:
                    await self.send(client, {"type": "tick", "seq": self.seq, **tick.__dict__})
                except websockets.ConnectionClosed:
                    pass

    async def heartbeat(self) -> None:
        """Gửi heartbeat server mỗi 10 giây tới mọi client đang kết nối."""

        while True:
            # Chu kỳ heartbeat độc lập với tốc độ replay giá.
            await asyncio.sleep(10)
            async with self.lock:
                # Copy danh sách client trong lock rồi gửi dữ liệu bên ngoài lock.
                clients = list(self.clients)
            for client in clients:
                try:
                    await self.send(client, {"type": "heartbeat", "timestamp": datetime.now().isoformat()})
                except websockets.ConnectionClosed:
                    pass

async def main() -> None:
    # Đọc tốc độ replay tùy chọn từ command line.
    parser = argparse.ArgumentParser()
    parser.add_argument("--speed", type=float, default=10)
    args = parser.parse_args()
    # Đọc dữ liệu demo/lịch sử từ feed-server/data.
    ticks = load_ticks(Path(__file__).parent / "data")
    if not ticks:
        raise SystemExit("No data: run python generate_data.py first")
    # Tạo một bộ điều phối replay dùng chung cho mọi WebSocket client.
    replay = Replay(ticks, args.speed)
    # Bind mọi interface để Android truy cập qua adb reverse hoặc LAN.
    async with websockets.serve(replay.handler, "0.0.0.0", 8765):
        print("ws://127.0.0.1:8765")
        # Chạy vô hạn market replay và heartbeat song song.
        await asyncio.gather(replay.replay(), replay.heartbeat())

if __name__ == "__main__":
    asyncio.run(main())
