const std = @import("std");
const testing = std.testing;

const Topic = @import("topic.zig").Topic;
const Message = @import("message.zig").Message;
const Consumer = @import("topic.zig").Consumer;

const FakeWriter = struct {
    received: bool = false,
    const Self = @This();
    pub fn write(self: *Self, _: Message) !void {
        self.received = true;
    }
};

test "subscribe and publish" {
    const allocator = testing.allocator;

    var topic = try Topic.init(allocator, "eventos", null, null);
    defer topic.deinit();

    var writer = FakeWriter{};

    const consumer = Consumer{
        .id = "client-1",
        .writer = &writer,
        .writerFn = struct {
            pub fn call(ctx: *anyopaque, msg: Message) anyerror!void {
                const w: *FakeWriter = @ptrCast(ctx);
                try w.write(msg);
            }
        }.call,
    };

    try topic.subscribe(consumer);

    const msg = Message{
        .topic = "events",
        .payload = "sending test",
        .timestamp = 1234567890,
    };

    try topic.publish(msg);

    try testing.expect(writer.received);
    try testing.expectEqual(@as(usize, 1), topic.messages.items.len);
}
