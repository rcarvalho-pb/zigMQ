const std = @import("std");
const print = std.debug.print;
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

    var topic = try Topic.init(allocator, "eventos", null);
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

test "create and topic in memory tmpDir" {
    const allocator = testing.allocator;
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();

    const filename = "topic-testing.log";
    const file = try tmp_dir.dir.createFile(filename, .{ .truncate = false, .read = true });

    const topic_name = "test-topic";
    var topic = try Topic.init(allocator, topic_name, .none);
    defer topic.deinit();
    topic.persistence = .file;
    topic.file = file;

    const msg = Message{
        .topic = topic_name,
        .payload = "hello world",
        .timestamp = 123456789,
    };

    try topic.publish(msg);
    file.close();
    topic.persistence = .none;
    topic.file = null;

    const full_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", tmp_dir.sub_path[0..], "topic-testing.log" });
    defer allocator.free(full_path);
    const reopenned = try std.fs.cwd().openFile(full_path, .{});

    var buffer: [256]u8 = undefined;
    const bytes_read = try reopenned.readAll(&buffer);
    const content = buffer[0..bytes_read];

    try testing.expect(std.mem.containsAtLeast(u8, "123456789|test-topic|hello world\n", 1, content));
}

test "create an topic and write it in default" {
    const allocator = testing.allocator;
    const topic_name = "test-topic";
    var topic = try Topic.init(allocator, topic_name, .file);
    defer topic.deinit();

    const msg = Message{
        .topic = topic_name,
        .payload = "topic test",
        .timestamp = 123456789,
    };

    try topic.publish(msg);

    const reopenned = try std.fs.cwd().openFile("logs/test-topic.log", .{ .mode = .read_only });
    defer reopenned.close();
    defer {
        std.fs.cwd().deleteFile("logs/test-topic.log") catch |err| {
            print("error deleting test file: {}\n", .{err});
        };
    }

    var buffer: [256]u8 = undefined;
    const bytes_read = try reopenned.readAll(&buffer);
    const content = buffer[0..bytes_read];

    try testing.expect(std.mem.containsAtLeast(u8, "123456789|test-topic|topic test\n", 1, content));
}
