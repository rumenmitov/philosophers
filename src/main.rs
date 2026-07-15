use std::{sync::*, thread, time::*, ops::*, collections::*, fmt};
use clap::Parser;


type Chopstick = usize;
type Chopsticks = (Chopstick, Chopstick);

struct EatRequest {
    philosopherid :usize,
    eat :Arc<(Mutex<Option<Chopsticks>>, Condvar)>
}

impl EatRequest {
    fn new(philosopherid :usize, 
        eat :Arc<(Mutex<Option<Chopsticks>>, Condvar)>) -> Self 
    {
        Self { philosopherid, eat }
    }
}

struct ThinkRequest {
    philosopherid :usize,
    chopsticks :Chopsticks
}

impl ThinkRequest {
    fn new(philosopherid :usize, chopsticks :Chopsticks) -> Self 
    {
        Self { philosopherid, chopsticks }
    }
}


enum WaiterMessage {
    Eat(EatRequest),
    Think(ThinkRequest),
    Quit
}

struct Data {
    thoughts :usize,
    meals :usize
}

impl Default for Data {
    fn default() -> Self {
        Self { 
            thoughts: 1,
            meals: 0
        }
    }
}

impl fmt::Display for Data {
    fn fmt(&self, f :&mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:>9} thoughts, {:>9} meals", self.thoughts, self.meals)
    }
}

struct Philosopher;

impl Philosopher {
    fn run(waiter :mpsc::Sender<WaiterMessage>, config :Arc<Args>) 
        -> Vec<thread::JoinHandle<()>> 
    {
        let mut handles :Vec<thread::JoinHandle<()>> = vec![];

        for i in 0..config.count {
            let mut philosopher = ThinkingPhilosopher::new(i, waiter.clone(), config.clone());
            let handle = thread::spawn(move || {
                loop {
                    let eating_philosopher = philosopher.think();
                    philosopher = eating_philosopher.eat();
                }
            });

            handles.push(handle);
        }

        handles
    }

    // chopstick to the left of the philosopher's own
    fn lchopstick(id :&usize, count :&usize) -> usize {
        assert!(id < count);

        if *id == 0 {
            count - 1
        } else {
            id - 1
        }
    }

    // chopstick to the right of the philosopher's own
    fn rchopstick(id :&usize, count :&usize) -> usize {
        assert!(id < count);

        if *id == count - 1 {
            0
        } else {
            id + 1
        }
    }
}


struct ThinkingPhilosopher {
    id :usize,
    waiter :mpsc::Sender<WaiterMessage>,
    eat_permission :Arc<(Mutex<Option<Chopsticks>>, Condvar)>,
    config :Arc<Args>
}


impl ThinkingPhilosopher {

    fn new(id :usize, waiter :mpsc::Sender<WaiterMessage>, 
        config :Arc<Args>) -> Self 
    {
        Self {
            id,
            waiter,
            eat_permission: Arc::new((Mutex::new(None), Condvar::new())),
            config
        }
    }

    fn think(self) -> EatingPhilosopher {
        let thinktime = rand::random::<u8>() % self.config.max_thinktime;

        if ! self.config.quiet {
            println!("Philosopher #{:0>3} is thinking...", self.id);
        }

        thread::sleep(Duration::from_secs(thinktime as u64));

        self.request_to_eat()
    }

    fn request_to_eat(self) -> EatingPhilosopher {
        let eat_request = EatRequest::new(self.id, self.eat_permission.clone());
        self.waiter.send(WaiterMessage::Eat(eat_request)).unwrap();

        let (eatlock, eatcvar) = &*self.eat_permission.clone();
        let mut can_eat = eatlock.lock().unwrap();

        while can_eat.deref().is_none() {
            can_eat = eatcvar.wait(can_eat).unwrap();
        }

        if let Some(chopsticks) = can_eat.take() {
            return EatingPhilosopher::new(self, chopsticks);
        }

        unreachable!()
    }
}

struct EatingPhilosopher {
    id :usize,
    waiter :mpsc::Sender<WaiterMessage>,
    chopsticks :Chopsticks,
    config :Arc<Args>
}

impl EatingPhilosopher {

    fn new(philosopher :ThinkingPhilosopher, chopsticks :Chopsticks) -> Self {
        Self { 
            id: philosopher.id, 
            waiter: philosopher.waiter, 
            chopsticks,
            config: philosopher.config
        }
    }

    fn eat(self) -> ThinkingPhilosopher {
        let eattime = rand::random::<u8>() % self.config.max_eattime;

        if ! self.config.quiet {
            println!("Philosopher #{:0>3} is eating!", self.id);
        }

        thread::sleep(Duration::from_secs(eattime as u64));

        self.finish_eating()
    }

    fn finish_eating(self) -> ThinkingPhilosopher {
        let think_request = ThinkRequest::new(self.id, self.chopsticks);
        self.waiter.send(WaiterMessage::Think(think_request)).unwrap();

        ThinkingPhilosopher::new(self.id, self.waiter, self.config)
    }
}

struct Waiter {
    config :Arc<Args>,
    requests :Vec<EatRequest>,
    chopsticks :HashSet<Chopstick>,
    bell :(mpsc::Sender<WaiterMessage>, mpsc::Receiver<WaiterMessage>),
    data :HashMap<usize, Data>,
}

impl Waiter {
    fn new(config :Arc<Args>) -> Self {
        Self {
            config: config.clone(),
            requests: vec![],
            chopsticks: (0..config.count).collect(),
            bell: mpsc::channel::<WaiterMessage>(),
            data: (0..config.count)
                .map(|i| (i, Data::default()))
                .collect(),
        }
    }

    fn run(&mut self) {
        loop {
            let (_, receiver) = &self.bell;
            let message = receiver.recv().unwrap();

            match message {
                WaiterMessage::Eat(eat_request) => self.handle_eat(eat_request),
                WaiterMessage::Think(think_request) => self.handle_think(think_request),
                WaiterMessage::Quit => self.handle_quit()
            };
        }
    }

    fn handle_eat(&mut self, eat_request :EatRequest) {
        self.requests.push(eat_request);
        self.serve_philosophers();
    }

    fn handle_think(&mut self, think_request :ThinkRequest) {
        let (chop1, chop2) = think_request.chopsticks;

        self.chopsticks.insert(chop1);
        self.chopsticks.insert(chop2);

        {
            let data = self.data.entry(think_request.philosopherid)
                .or_insert(Data::default());

            (*data).thoughts += 1;
        }

        self.serve_philosophers();
    }

    fn handle_quit(&self) {
        let mut philosopherids :Vec<usize> = self.data.keys().copied().collect();
        philosopherids.sort();

        println!("\n{:>19}*** SUMMARY ***", "");
        for id in philosopherids {
            println!("Philosopher #{:0>3}: {}", id, self.data.get(&id).unwrap());
        }

        std::process::exit(0);
    }

    fn serve_philosophers(&mut self) {
        let mut i = 0;

        while i < self.requests.len() {
            let current_chop = self.requests[i].philosopherid;
            let lchop = Philosopher::lchopstick(&current_chop, &self.config.count);
            let rchop = Philosopher::rchopstick(&current_chop, &self.config.count);

            if self.chopsticks.contains(&current_chop) &&
                (self.chopsticks.contains(&lchop) ||
                 self.chopsticks.contains(&rchop))
            {
                // NOTE We want to preserve the order of requests to prevent
                // starvation.
                let req = self.requests.remove(i);

                let chop1 = self.chopsticks.take(&current_chop).unwrap();
                let chop2 = match self.chopsticks.take(&rchop) {
                    Some(chopstick) => chopstick,
                    None => self.chopsticks.take(&lchop).unwrap()
                };

                {
                    let data = self.data.entry(req.philosopherid)
                        .or_insert(Data::default());

                    (*data).meals += 1;
                }

                {
                    let (eatlock, eatcvar) = &*req.eat;
                    let mut can_eat = eatlock.lock().unwrap();

                    *can_eat = Some((chop1, chop2));
                    eatcvar.notify_one();
                }

            } else {
                i += 1;
            }
        }
    }
}

/// Program that simulates the Dining Philosophers Problem
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {

    /// Suppress verbose output
    #[arg(short, long, default_value_t = false)]
    quiet: bool,

    /// Total number of philosophers
    #[arg(short, long, default_value_t = 5)]
    count: usize,

    /// Maximum time a philsopher spends eating (in seconds)
    #[arg(long, default_value_t = 5)]
    max_eattime: u8,

    /// Maximum time a philsopher spends thinking (in seconds)
    #[arg(long, default_value_t = 5)]
    max_thinktime: u8
}


fn main() {
    let args = Arc::new(Args::parse());

    println!("{:>15}========================", "");
    println!("{:>15}  DINING PHILOSOPHERS!  ", "");
    println!("{:>15}========================", "");

    let mut waiter = Waiter::new(args.clone());
    let waiter_tx = waiter.bell.0.clone();
    let control_tx = waiter.bell.0.clone();

    let waiter_handle = thread::spawn(move || waiter.run());

    ctrlc::set_handler(move || 
        control_tx.send(WaiterMessage::Quit).unwrap()
    ).unwrap();

    let handles :Vec<thread::JoinHandle<()>> = Philosopher::run(waiter_tx, args.clone());

    for handle in handles {
        let _ = handle.join();
    }

    let _ = waiter_handle.join();
}
