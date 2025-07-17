const std = @import("std");
const Print = std.debug.print;

const ErrorCommand = error{
    NotFound,
    Invalid,
};

const CommandType = enum {
    subscribe,
    unsubscribe,
    publish,
};

const Command = union(CommandType) {
    subscribe: struct {
        topic: []const u8,
    },
    unsubscribe: struct {
        topic: []const u8,
    },
    publish: struct {
        topic: []const u8,
        message: []const u8,
    },
};

pub fn parseCommand(line: []const u8) anyerror!Command {
    var it = std.mem.tokenizeAny(u8, line, " ");
    const command = it.next() orelse return ErrorCommand.Invalid;
    const cmdType = std.meta.stringToEnum(CommandType, command) orelse return ErrorCommand.NotFound;
    switch (cmdType) {
        .subscribe => {
            return Command{ .subscribe = .{
                .topic = trim(it.next() orelse return ErrorCommand.Invalid),
            } };
        },
        .unsubscribe => {
            return Command{ .unsubscribe = .{
                .topic = trim(it.next() orelse return ErrorCommand.Invalid),
            } };
        },
        .publish => {
            return Command{
                .publish = .{
                    .topic = trim(it.next() orelse return ErrorCommand.Invalid),
                    .message = trim(it.rest()),
                },
            };
        },
    }
}

fn trim(input: []const u8) []const u8 {
    var start: usize = 0;
    var end: usize = input.len;

    // Avança start enquanto for espaço, '\n' ou '\r'
    while (start < end and
        (input[start] == ' ' or input[start] == '\n' or input[start] == '\r'))
    {
        start += 1;
    }

    // Recuar end enquanto for espaço, '\n' ou '\r'
    while (end > start and
        (input[end - 1] == ' ' or input[end - 1] == '\n' or input[end - 1] == '\r'))
    {
        end -= 1;
    }

    return input[start..end];
}
