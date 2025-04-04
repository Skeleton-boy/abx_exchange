const net = require("net");

const PORT = 3000;
const HOST = "127.0.0.1";

const packets = [
    { symbol: "AAPL", buysellindicator: "B", quantity: 50, price: 100, packetSequence: 1 },
    { symbol: "AAPL", buysellindicator: "B", quantity: 30, price: 98, packetSequence: 2 },
    { symbol: "AAPL", buysellindicator: "S", quantity: 20, price: 101, packetSequence: 3 },
    { symbol: "AAPL", buysellindicator: "S", quantity: 10, price: 102, packetSequence: 4 },
    { symbol: "META", buysellindicator: "B", quantity: 40, price: 50, packetSequence: 5 },
    { symbol: "META", buysellindicator: "S", quantity: 30, price: 55, packetSequence: 6 },
    { symbol: "META", buysellindicator: "S", quantity: 20, price: 57, packetSequence: 7 },
    { symbol: "MSFT", buysellindicator: "B", quantity: 25, price: 150, packetSequence: 8 },
    { symbol: "MSFT", buysellindicator: "S", quantity: 15, price: 155, packetSequence: 9 },
    { symbol: "MSFT", buysellindicator: "B", quantity: 20, price: 148, packetSequence: 10 },
    { symbol: "AMZN", buysellindicator: "B", quantity: 10, price: 3000, packetSequence: 11 },
    { symbol: "AMZN", buysellindicator: "B", quantity: 5, price: 2999, packetSequence: 12 },
    { symbol: "AMZN", buysellindicator: "S", quantity: 15, price: 3020, packetSequence: 13 },
    { symbol: "AMZN", buysellindicator: "S", quantity: 10, price: 3015, packetSequence: 14 }
];

function createPacketBuffer(packet) {
    let buffer = Buffer.alloc(17);
    buffer.write(packet.symbol.padEnd(4, " "), 0, 4, "ascii");
    buffer.write(packet.buysellindicator, 4, 1, "ascii");
    buffer.writeUInt32BE(packet.quantity, 5);
    buffer.writeUInt32BE(packet.price, 9);
    buffer.writeUInt32BE(packet.packetSequence, 13);
    return buffer;
}

const server = net.createServer((socket) => {
    console.log("Client connected.");

    socket.on("error", (err) => {
        if (err.code === "ECONNRESET") {
            console.log("Client disconnected unexpectedly (ECONNRESET).");
        } else {
            console.error("Socket error:", err.message);
        }
    });

    packets.forEach((packet) => {
        if (Math.random() > 0.75) {
            console.log(`Simulating loss for packet ${packet.packetSequence}`);
            return;
        }
        if (socket.writable) {
            socket.write(createPacketBuffer(packet));
        }
    });

    console.log("Packets sent. Waiting for resend requests...");

    socket.on("data", (data) => {
        if (data.length < 2) return;

        const callType = data.readUInt8(0);
        const resendSeq = data.readUInt8(1);

        if (callType === 2) {
            const missingPacket = packets.find(p => p.packetSequence === resendSeq);
            if (missingPacket) {
                console.log(`Resending packet sequence ${resendSeq}`);

                if (socket.writable) {
                    socket.write(createPacketBuffer(missingPacket));
                } else {
                    console.log(`Error: Tried to send packet ${resendSeq}, but socket is closed.`);
                }
            } else {
                console.log(`Requested missing sequence ${resendSeq} not found.`);
            }
        }
    });

    socket.on("end", () => {
        console.log("Client disconnected.");
    });
});

server.listen(PORT, HOST, () => {
    console.log(`TCP server started on ${HOST}:${PORT}`);
});
