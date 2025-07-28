const std = @import("std");
const print = std.debug.print;
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;

const Message = @import("message.zig").Message;

pub const PersistenceMode = enum {
    none,
    file,
};

pub const Consumer = struct {
    id: []const u8,
    writer: *anyopaque,
    writerFn: *const fn (ctx: *anyopaque, msg: Message) anyerror!void,
    queue: ArrayList(*Message),

    const Self = @This();

    pub fn init(allocator: Allocator, id: []const u8, writer: *anyopaque, writerFn: *const fn (*anyopaque, Message) anyerror!void) !Consumer {
        return Consumer{
            .id = id,
            .writer = writer,
            .writerFn = writerFn,
            .queue = std.ArrayList(*Message).init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        // for (self.queue.items) |m| {
        //     self.queue.allocator.destroy(m);
        // }
        self.queue.deinit();
    }
};

pub const Topic = struct {
    allocator: Allocator,
    name: []const u8,
    messages: ArrayList(*Message),
    consumers: ArrayList(*Consumer),
    persistence: PersistenceMode,
    file: ?std.fs.File = null,

    pub fn init(allocator: Allocator, name: []const u8, persistence: ?PersistenceMode) !Topic {
        const mode = persistence orelse .none;
        var topic = Topic{
            .allocator = allocator,
            .name = name,
            .messages = ArrayList(*Message).init(allocator),
            .consumers = ArrayList(*Consumer).init(allocator),
            .persistence = mode,
        };

        const getOrCreateFile = struct {
            pub fn call(path: []const u8) anyerror!std.fs.File {
                const file = std.fs.cwd().openFile(path, .{ .mode = .read_write }) catch |err| blk: {
                    if (err == error.FileNotFound) {
                        break :blk try std.fs.cwd().createFile(path, .{ .truncate = false, .read = false });
                    } else {
                        return err;
                    }
                };
                return file;
            }
        }.call;

        switch (mode) {
            .file => {
                const dir_path = "logs";
                const filename = try std.fmt.allocPrint(allocator, "{s}.log", .{name});
                defer allocator.free(filename);
                const path = try std.fs.path.join(allocator, &.{ dir_path, filename });
                defer allocator.free(path);
                var dir = std.fs.cwd().openDir(dir_path, .{}) catch |err| blk: {
                    if (err == error.FileNotFound) {
                        try std.fs.cwd().makeDir(dir_path);
                        break :blk try std.fs.cwd().openDir(dir_path, .{});
                    } else {
                        return err;
                    }
                };
                dir.close();
                const file = try getOrCreateFile(path);
                topic.file = file;
            },
            .none => {},
        }
        return topic;
    }

    pub fn deinit(self: *@This()) void {
        for (self.messages.items) |m| {
            self.allocator.destroy(m);
        }
        for (self.consumers.items) |c| {
            c.deinit();
            self.allocator.destroy(c);
        }
        if (self.file) |f| {
            f.close();
        }
        self.messages.deinit();
        self.consumers.deinit();
    }

    pub fn subscribe(self: *@This(), consumer: Consumer) !void {
        const consumerPtr = try self.allocator.create(Consumer);
        consumerPtr.* = try Consumer.init(self.allocator, consumer.id, consumer.writer, consumer.writerFn);
        try self.consumers.append(consumerPtr);
    }

    pub fn unsubscribe(self: *@This(), consumer_id: []const u8) void {
        for (self.consumers.items, 0..) |c, i| {
            if (std.mem.eql(u8, c.id, consumer_id)) {
                const consumer = self.consumers.swapRemove(i);
                self.allocator.destroy(consumer);
                break;
            }
        }
    }

    pub fn listConsumers(self: @This()) !std.ArrayList([]const u8) {
        var list = std.ArrayList([]const u8).init(self.allocator);
        for (self.consumers.items) |c| {
            try list.append(c.id);
        }
        return list;
    }

    pub fn publish(self: *@This(), msg: Message) !void {
        const msgPtr = try self.allocator.create(Message);
        msgPtr.* = msg;
        try self.messages.append(msgPtr);
        for (self.consumers.items) |c| {
            try c.writerFn(c.writer, msg);
            try c.queue.append(msgPtr);
        }
        if (self.persistence == .file) {
            if (self.file) |f| {
                const to_write = try std.fmt.allocPrint(self.allocator, "{d}|{s}|{s}\n", .{
                    msg.timestamp,
                    msg.topic,
                    msg.payload,
                });
                defer self.allocator.free(to_write);
                try f.writeAll(to_write);
            }
        }
    }
};
