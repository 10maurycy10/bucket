use std::env;
use serenity::async_trait;
use serenity::model::channel::Message;
use serenity::model::gateway::Ready;
use serenity::prelude::*;
use rusqlite::{params, Connection, Result};
use rusqlite::NO_PARAMS;
use std::sync::Arc;

struct Handler {
    conn: Arc<Mutex<Connection>>
}

#[async_trait]
impl EventHandler for Handler {
    async fn message(&self, ctx: Context, msg: Message) {
        if msg.content.starts_with("bucket") {
            let split: Vec<_> = msg.content.split(" ").filter(|x| x.len() > 0).collect();
            if split.get(1) == Some(&"add") {
                let name = &split[2];
                let fact = &split[4..];
                if fact.len() > 256 || name.len() > 32 {
                    if let Err(why) = msg.channel_id.say(&ctx.http, "hey, no books!").await {
                        println!("Error sending message: {:?}", why);
                    }            
                } else {
                    self.conn.lock().await.execute(
                        "INSERT INTO facts (topic, text, id) values (?1,?2, ?3)",
                         &[name, fact.join(" ").as_str(), &format!("{}", std::time::SystemTime::now().duration_since(std::time::SystemTime::UNIX_EPOCH).unwrap().as_secs()).as_str()],
                     ).unwrap();
                }
                println!("{:?} {:?}", name, fact);
            } else if split.get(1) == Some(&"del") {
                if let Some(id) = split.get(2) {
                    self.conn.lock().await.execute(
                        "DELETE from facts where id=?1;",
                        &[id]
                    ).unwrap();
                }
            } else if split.get(1) == Some(&"ping") {
                if let Err(why) = msg.channel_id.say(&ctx.http, "Pong!").await {
                    println!("Error sending message: {:?}", why);
                }
            } else if split.get(1) == Some(&"help") {
                if let Err(why) = msg.channel_id.say(&ctx.http, "commands:\nbucket ping - bot will reply with 'Pong!'\nbucket add <topic> is <fact> - adds a fact\nbucket random - shows a random fact\nbucket flush - writes the database out to disk\nbucket [get] topic - shows facts on a topic\nbucket del <id> - deletes a fact").await {
                    println!("Error sending message: {:?}", why);
                }
            } else if split.get(1) == Some(&"flush") {
                self.conn.lock().await.cache_flush().unwrap();
                println!("DB flushed")
            } else if split.get(1) == Some(&"random") {
                let conn = self.conn.clone();
                let facts = tokio::task::spawn_blocking(move || {
                    let conn = conn.blocking_lock();
                    let mut stmt = conn.prepare("SELECT text,topic FROM facts ORDER BY RANDOM() LIMIT 1;").unwrap();
                    let facts = stmt.query_map([],|row| Ok((row.get_unwrap::<usize,String>(0), row.get_unwrap::<usize,String>(1)))).unwrap().map(|x| x.unwrap());
                    let facts: Vec<(String,String)> = facts.collect();
                    facts
                }).await.unwrap();
                if facts.len() > 0 {
                    if let Err(why) = msg.channel_id.say(&ctx.http, format!(" - {} is {}", facts[0].1, facts[0].0)).await {
                         println!("Error sending message: {:?}", why);
                    }
                } else {
                    if let Err(why) = msg.channel_id.say(&ctx.http, "No facts to show").await {
                         println!("Error sending message: {:?}", why);
                    }
                }
            } else if let Some(first) = split.get(1) {
                let topic = if first == &"get" {
                    match split.get(2) {
                        Some(x) => x,
                        None => return
                    }
                } else {
                    first
                };
                let topic2 = topic.to_string();
                let conn = self.conn.clone();
                println!("topic: {:?}", &topic);
                let facts = tokio::task::spawn_blocking(move || {
                    let conn = conn.blocking_lock();
                    let mut stmt = conn.prepare("SELECT text,id FROM facts WHERE topic=?1 ORDER BY RANDOM() LIMIT 7;").unwrap();
                    let facts = stmt.query_map([topic2],|row| Ok((row.get_unwrap::<usize,String>(0), row.get_unwrap::<usize,u64>(1)))).unwrap().map(|x| x.unwrap());
                    let facts: Vec<(String,u64)> = facts.collect();
                    facts
                }).await.unwrap();
                let mut msgbuf = String::new();
                for i in 0..facts.len() {
                    msgbuf.push_str(&format!(" - {} is {} ({})\n", &topic, facts[i].0, facts[i].1));
                }
                if facts.len() > 0 {
                    if let Err(why) = msg.channel_id.say(&ctx.http, format!("Bucket has {} facts:\n{}", facts.len(), msgbuf)).await {
                         println!("Error sending message: {:?}", why);
                    }
                } else {
                    if let Err(why) = msg.channel_id.say(&ctx.http, "no facts on that topic :(").await {
                         println!("Error sending message: {:?}", why);
                    }
                }
            }
        }
    }

    // Set a handler to be called on the `ready` event. This is called when a
    // shard is booted, and a READY payload is sent by Discord. This payload
    // contains data like the current user's guild Ids, current user data,
    // private channels, and more.
    //
    // In this case, just print what the current user's username is.
    async fn ready(&self, _: Context, ready: Ready) {
        println!("{} is connected!", ready.user.name);
    }
}

#[tokio::main]
async fn main() {
    // open database connection
    let conn = Connection::open("db.sqlite").unwrap(); 

    conn.execute(
        "create table if not exists facts (
            userid VARCHAR(64),
            text TEXT,
            id INT,
            topic TEXT
        )",
        NO_PARAMS,
    ).unwrap();

    // Configure the client with your Discord bot token in the environment.
    let token = env::var("DISCORD_TOKEN").expect("Expected a token in the environment");
    // Set gateway intents, which decides what events the bot will be notified about
    let intents = GatewayIntents::GUILD_MESSAGES
        | GatewayIntents::DIRECT_MESSAGES
        | GatewayIntents::MESSAGE_CONTENT;

    // Create a new instance of the Client, logging in as a bot. This will
    // automatically prepend your bot token with "Bot ", which is a requirement
    // by Discord for bot users.
    let mut client =
        Client::builder(&token, intents).event_handler(Handler {conn: Arc::new(Mutex::new(conn))}).await.expect("Err creating client");

    // Finally, start a single shard, and start listening to events.
    //
    // Shards will automatically attempt to reconnect, and will perform
    // exponential backoff until it reconnects.
    if let Err(why) = client.start().await {
        println!("Client error: {:?}", why);
    }
}
