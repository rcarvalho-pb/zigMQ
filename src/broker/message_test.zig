const std = @import("std");
const testing = std.testing;
const Message = @import("message.zig").Message;

test "test create message successfully" {
    const topic = "Topic Name";
    const payload = "Just another payload";

    const msg = Message{
        .topic = topic,
        .payload = payload,
        .timestamp = 1721400000,
    };

    try testing.expectEqual(@as(i64, 1721400000), msg.timestamp);
    try testing.expectEqualStrings(topic, msg.topic);
    try testing.expectEqualStrings(payload, msg.payload);
}
