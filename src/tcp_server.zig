const std = @import("std");
const net = std.net;
const Print = std.debug.print;

const Handle = @import("client_handler.zig").handle;
const Broker = @import("broker.zig").Broker;

const Allocator = std.mem.Allocator;
const Thread = std.Thread;

const PORT = 8080;

pub fn runServer(allocator: Allocator, broker: *Broker) !void {
    var addr = try net.Address.resolveIp("127.0.0.1", PORT);
    var srv = try addr.listen(.{ .reuse_address = true });
    Print("server started on port [{d}]\n", .{PORT});
    while (true) {
        const conn = &try srv.accept();
        var ip_buf: [40]u8 = undefined;
        const ip = formatIp(conn.address, ip_buf[0..]);
        Print("New client connected on: {s}:{d}\n", .{ ip, conn.address.getPort() });
        _ = try Thread.spawn(.{}, Handle, .{ allocator, broker, conn });
    }
}

pub fn formatIp(addr: net.Address, buffer: []u8) []const u8 {
    const ip_bytes = blk: {
        var out: [4]u8 = undefined;
        std.mem.writeInt(u32, &out, addr.in.sa.addr, .little);
        break :blk out;
    };
    const result = std.fmt.bufPrint(buffer, "{}.{}.{}.{}", .{
        ip_bytes[0],
        ip_bytes[1],
        ip_bytes[2],
        ip_bytes[3],
    }) catch return "[format error]";
    return result;
}
