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

    var topic = try Topic.init(allocator, "topic-test", null);
    defer topic.deinit();

    var writer = FakeWriter{};
    var consumer = Consumer{
        .id = "client-1",
        .writer = &writer,
        .writerFn = struct {
            pub fn call(ctx: *anyopaque, msg: Message) anyerror!void {
                const w: *FakeWriter = @ptrCast(@alignCast(ctx));
                try w.write(msg);
            }
        }.call,
        .queue = std.ArrayList(*Message).init(allocator),
    };
    defer consumer.deinit();

    try topic.subscribe(consumer);

    const msg = Message{
        .topic = "topic-test",
        .payload = "testing publish and flush",
        .timestamp = 1234,
    };

    try topic.publish(msg);

    try topic.flushAll();

    try testing.expect(writer.received);
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
    const filename = try std.fmt.allocPrint(allocator, "{s}.log", .{topic_name});
    defer allocator.free(filename);
    const full_path = try std.fs.path.join(allocator, &.{ "logs", filename });
    defer allocator.free(full_path);
    var topic = try Topic.init(allocator, topic_name, .file);
    defer topic.deinit();

    const msg = Message{
        .topic = topic_name,
        .payload = "topic test",
        .timestamp = 123456789,
    };

    try topic.publish(msg);

    const reopenned = try std.fs.cwd().openFile(full_path, .{ .mode = .read_only });
    defer reopenned.close();
    defer {
        std.fs.cwd().deleteFile(full_path) catch |err| {
            print("error deleting test file: {}\n", .{err});
        };
    }

    var buffer: [256]u8 = undefined;
    const bytes_read = try reopenned.readAll(&buffer);
    const content = buffer[0..bytes_read];

    try testing.expect(std.mem.containsAtLeast(u8, "123456789|test-topic|topic test\n", 1, content));
}

test "consumer flushes queue and receives message" {
    const allocator = testing.allocator;

    var topic = try Topic.init(allocator, "flush-topic", null);
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
        .queue = std.ArrayList(*Message).init(allocator),
    };
    defer consumer.queue.deinit();

    try topic.subscribe(consumer);

    const msg = Message{
        .topic = "flush-topic",
        .payload = "message in queue",
        .timestamp = 1234,
    };

    try topic.publish(msg);

    try testing.expect(!writer.received);

    const c_ptr = topic.consumers.items[0];
    try c_ptr.flush();

    try testing.expect(writer.received);
}

test "replay previously pusblished messages" {
    const allocator = testing.allocator;

    var topic = try Topic.init(allocator, "replay-topic", null);
    defer topic.deinit();

    const msg1 = Message{ .topic = "replay-topic", .payload = "first", .timestamp = 1234 };
    const msg2 = Message{ .topic = "replay-topic", .payload = "second", .timestamp = 1235 };

    try topic.publish(msg1);
    try topic.publish(msg2);

    var buffer = std.ArrayList(Message).init(allocator);
    defer buffer.deinit();

    var consumer = Consumer{
        .id = "replayer",
        .writer = &buffer,
        .writerFn = struct {
            pub fn call(ctx: *anyopaque, msg: Message) anyerror!void {
                const list: *std.ArrayList(Message) = @ptrCast(@alignCast(ctx));
                try list.append(msg);
                for (list.items) |m| {
                    print("Message in list: {s}\n", .{m.topic});
                }
            }
        }.call,
        .queue = std.ArrayList(*Message).init(allocator),
    };
    defer consumer.deinit();

    try topic.subscribe(consumer);
    try topic.replay("replayer");

    try testing.expectEqual(@as(usize, 2), buffer.items.len);
    try testing.expectEqualStrings("first", buffer.items[0].payload);
    try testing.expectEqualStrings("second", buffer.items[1].payload);
}
