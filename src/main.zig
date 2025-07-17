const std = @import("std");
const Print = std.debug.print;

const Broker = @import("broker.zig").Broker;
const ParseMessage = @import("message.zig").parseCommand;
const RunServer = @import("tcp_server.zig").runServer;

const Allocator = std.mem.Allocator;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var broker = Broker.init(allocator);

    try RunServer(allocator, &broker);
}
