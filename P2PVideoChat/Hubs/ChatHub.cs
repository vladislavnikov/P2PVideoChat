using System.Collections.Concurrent;
using Microsoft.AspNetCore.SignalR;

namespace P2PVideoChat.Hubs
{
    public interface IChatClient
    {
        Task ReceiveMessage(string user, string message);
        Task SystemMessage(string message);
        Task RoomFull(string room);
    }

    public class ChatHub : Hub<IChatClient>
    {
        private const int MaxRoomSize = 2;
        private static readonly ConcurrentDictionary<string, HashSet<string>> Rooms = new();
        private static readonly ConcurrentDictionary<string, string> Users = new();

        public async Task<bool> JoinRoom(string room, string user)
        {
            user = user.Trim();
            if (user.Length == 0) user = "Anonymous";

            var members = Rooms.GetOrAdd(room, _ => new HashSet<string>());

            bool added;
            lock (members)
            {
                added = members.Count < MaxRoomSize || members.Contains(Context.ConnectionId);
                if (added) members.Add(Context.ConnectionId);
            }

            if (!added)
            {
                await Clients.Caller.RoomFull(room);
                return false;
            }

            Users[Context.ConnectionId] = user;
            await Groups.AddToGroupAsync(Context.ConnectionId, room);
            await Clients.OthersInGroup(room).SystemMessage($"{user} joined");
            return true;
        }

        public async Task SendMessage(string message)
        {
            var room = FindRoom(Context.ConnectionId);
            if (room != null && Users.TryGetValue(Context.ConnectionId, out var user))
                await Clients.Group(room).ReceiveMessage(user, message);
        }

        public override async Task OnDisconnectedAsync(Exception? exception)
        {
            var room = FindRoom(Context.ConnectionId);
            Users.TryRemove(Context.ConnectionId, out var user);
            if (room != null && Rooms.TryGetValue(room, out var members))
            {
                lock (members) members.Remove(Context.ConnectionId);
                await Clients.OthersInGroup(room).SystemMessage($"{user ?? "Someone"} left");
            }
            await base.OnDisconnectedAsync(exception);
        }

        private static string? FindRoom(string connectionId)
        {
            foreach (var (room, members) in Rooms)
                lock (members)
                    if (members.Contains(connectionId))
                        return room;
            return null;
        }
    }
}