using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Data.Entity;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Chronic;
using Microsoft.Bot.Builder.History;
using Microsoft.Bot.Connector;
using Microsoft.Data.OData;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;

namespace Microsoft.Bot.Builder.Azure.Logging
{
    public class ConversationEntry
    {
        public string ChannelId { get; set; } 
        public string ConversationId { get; set; }
        public List<ActivityEntry>  Activities { get; set; }
    }

    public class ActivityEntry
    {
        [ForeignKey("Conversation")]
        public int Conversation_Id { get; set; }
        public ConversationEntry Conversation { get; set; }
        public DateTime Timestamp { get; set; }
        /// <summary>
        /// Version number for the underlying activity.
        /// </summary>
        public double Version { get; set; }

        /// <summary>
        /// Channel identifier for sender.
        /// </summary>
        public string From { get; set; }

        /// <summary>
        /// Channel identifier for receiver.
        /// </summary>
        public string Recipient { get; set; }

        /// <summary>
        /// Logged activity.
        /// </summary>
        [NotMapped]
        public IActivity Activity { get; set; }

        public string ActivityJson { get; set; }
    }

    public interface ILoggingDBContext
    {
       DbSet<ActivityEntry>  Activities { get; set; }
       DbSet<ConversationEntry> Conversations { get; set; }
       void SaveChanges();
    }


    public class EntityFrameworkLogger : IActivityLogger, IActivitySource, IActivityManager
    {
        private ILoggingDBContext _loggingDbContext;
        private EntityFrameworkLoggerSettings _entityFrameworkLoggerSettings;

        public EntityFrameworkLogger(ILoggingDBContext context, EntityFrameworkLoggerSettings settings)
        {
            _entityFrameworkLoggerSettings = settings;
            _loggingDbContext = context;
        }
        public async Task LogAsync(IActivity activity)
        {
            ActivityEntry entry = new ActivityEntry();
            //TODO: map using AutoFac
            _loggingDbContext.Activities.Add(entry);
            
            // save changes, if auto commit
            if (_entityFrameworkLoggerSettings.AutoCommit)
                _loggingDbContext.SaveChanges();
        }

        public IEnumerable<IActivity> Activities(string channelId, string conversationId, DateTime oldest = new DateTime())
        {
            List<Activity> activityList = new List<Activity>();
           //get a of relevant conversations
            var activities = (from conversation in _loggingDbContext.Conversations.Include(c => c.Activities)
                where conversation.ChannelId == channelId &&
                      conversation.ConversationId == conversationId
                select conversation.Activities).FirstOrDefault();

            var filteredActivities = from activity in activities where activity.Timestamp < oldest select activity;

            //now we need to generate rehydrated objects
            foreach (var filteredActivity in filteredActivities)
            {
                activityList.Add(JsonConvert.DeserializeObject<Activity>(filteredActivity.ActivityJson));
            }

            return activityList;

            //filter on activities
        }

        public Task WalkActivitiesAsync(Func<IActivity, Task> function, string channelId = null, string conversationId = null,
            DateTime oldest = new DateTime(), CancellationToken cancel = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public async Task DeleteConversationAsync(string channelId, string conversationId,
            CancellationToken cancel = new CancellationToken())
        {
            //find
            var toBeDeletedList = from conversation in _loggingDbContext.Conversations
                where conversation.ChannelId == channelId &&
                      conversation.ConversationId == conversationId
                select conversation;
            //remove
            toBeDeletedList.ForEach( entry => _loggingDbContext.Conversations.Remove(entry));
            
            if (_entityFrameworkLoggerSettings.AutoCommit)
                _loggingDbContext.SaveChanges();
        }

        public Task DeleteBeforeAsync(DateTime oldest, CancellationToken cancel = new CancellationToken())
        {
            throw new NotImplementedException();
        }

        public Task DeleteUserActivitiesAsync(string userId, CancellationToken cancel = new CancellationToken())
        {
           var task =  Task.Run(() =>
           {
               //find all activities with the recipient.
               var toBeDeletedList = (from activity in _loggingDbContext.Activities
                                      where activity.Recipient == userId
                                      select activity)
                   //avoid changing the cursor while iterating
                   .ToList();

               // first remove all Activities belonging to the user
               toBeDeletedList.ForEach(a => _loggingDbContext.Activities.Remove(a));
               // now remove all conversations for the user.
               toBeDeletedList.Select(c => c.Conversation_Id)
                   .Distinct()
                   .ForEach(a => _loggingDbContext.Conversations
                   .Remove(_loggingDbContext.Conversations.Find(a)));

               if (_entityFrameworkLoggerSettings.AutoCommit)
                   _loggingDbContext.SaveChanges();


           });
            return task;
        }
    }
}
