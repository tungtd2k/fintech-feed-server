from __future__ import annotations

# File dự phòng này sinh nến demo ổn định khi không dùng được nguồn giá thật.
# Nó không làm thay đổi cấu trúc protocol WebSocket.
import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

SYMBOLS = {"FPT": 145000, "HPG": 27500, "VCB": 92000, "VIC": 41000, "VNM": 68000}

def main() -> None:
    # Lưu một file CSV cho mỗi mã cạnh file script này.
    output = Path(__file__).parent / "data"
    output.mkdir(exist_ok=True)
    # Seed cố định giúp mỗi lần demo cho cùng dữ liệu để debug/chụp màn hình.
    random.seed(20261114)
    # Sinh khoảng 15 ngày giao dịch với nến một phút.
    start = datetime.now().replace(hour=9, minute=0, second=0, microsecond=0) - timedelta(days=15)
    for symbol, initial in SYMBOLS.items():
        # Mỗi mã bắt đầu từ giá tham chiếu đã cấu hình.
        price = float(initial)
        with (output / f"{symbol}.csv").open("w", newline="", encoding="utf-8") as file:
            # Các cột CSV khớp trực tiếp với DTO Tick của backend.
            writer = csv.DictWriter(file, fieldnames=["symbol", "timestamp", "open", "high", "low", "close", "volume"])
            writer.writeheader()
            current = start
            for _ in range(15 * 390):
                # Bỏ qua giờ nghỉ trưa trong phiên giao dịch Việt Nam.
                if current.hour == 12:
                    current += timedelta(hours=1)
                # Giá mở là giá đóng trước đó; giá đóng dùng random walk giới hạn.
                opening = price
                closing = max(1, price + random.gauss(0, max(price * 0.0008, 1)))
                # Cao/thấp tạo phần râu nến quanh giá mở và giá đóng.
                high = max(opening, closing) + abs(random.gauss(0, price * 0.0003))
                low = max(1, min(opening, closing) - abs(random.gauss(0, price * 0.0003)))
                # Ghi một nến một phút chuẩn hóa cho replay server.
                writer.writerow({"symbol": symbol, "timestamp": current.isoformat(), "open": round(opening, 2), "high": round(high, 2), "low": round(low, 2), "close": round(closing, 2), "volume": random.randint(100, 25000)})
                price = closing
                current += timedelta(minutes=1)
                # Hết phiên thì chuyển sang ngày tiếp theo lúc mở cửa.
                if current.hour >= 16:
                    current = current.replace(hour=9, minute=0) + timedelta(days=1)

if __name__ == "__main__":
    main()
