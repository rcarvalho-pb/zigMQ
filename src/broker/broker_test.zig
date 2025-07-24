const std = @import("std");
const testing = std.testing;

const Broker = @import("broker.zig").Broker;
const Message = @import("message.zig").Message;
const Consumer = @import("topic.zig").Consumer;

test {
    _ = @import("message_test.zig");
    _ = @import("topic_test.zig");
}

const FakeWriter = struct {
    received: bool = false,
    quantity: i8 = 0,
    pub fn write(self: *@This(), _: Message) !void {
        self.received = true;
        self.quantity += 1;
    }
};

test "consumer can subscribe to multiple topics" {
    const allocator = testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();
    var writer = FakeWriter{};
    const consumer = Consumer{
        .id = "client-1",
        .writer = &writer,
        .writerFn = struct {
            pub fn call(ctx: *anyopaque, msg: Message) !void {
                const w: *FakeWriter = @ptrCast(ctx);
                try w.write(msg);
            }
        }.call,
    };

    const msg = Message{
        .topic = "topic test",
        .payload = "testing payload",
        .timestamp = 1234567890,
    };

    try broker.subscribe("topic-1", consumer);
    try broker.subscribe("topic-2", consumer);

    var it = broker.topics.iterator();
    while (it.next()) |entry| {
        try entry.value_ptr.*.publish(msg);
    }

    try testing.expectEqual(@as(i8, 2), writer.quantity);
    try testing.expect(writer.received);
    it = broker.topics.iterator();
    while (it.next()) |entry| {
        try testing.expectEqual(@as(usize, 1), entry.value_ptr.*.messages.items.len);
    }
}

test "broker publishes messages to a topic" {
    const allocator = testing.allocator;
    var broker = Broker.init(allocator);
    defer broker.deinit();

    var writer = FakeWriter{};

    const clientID = "client-1";
    const consumer = Consumer{
        .id = clientID,
        .writer = &writer,
        .writerFn = struct {
            pub fn call(ctx: *anyopaque, msg: Message) !void {
                const w: *FakeWriter = @ptrCast(ctx);
                try w.write(msg);
            }
        }.call,
    };

    const topic_name = "topic-1";
    const msg = Message{
        .topic = topic_name,
        .payload = "Payload test",
        .timestamp = 123,
    };

    try broker.subscribe(topic_name, consumer);
    try broker.publish(topic_name, msg);

    const topic = broker.topics.get(topic_name).?;

    try testing.expectEqualStrings(topic_name, topic.name);
    try testing.expectEqual(@as(i8, 1), writer.quantity);
    try testing.expect(writer.received);
}
