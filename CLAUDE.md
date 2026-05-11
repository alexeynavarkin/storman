## Core principles

- Stop telling me "You're right", it just shows how incompetent you are. Do it right on your first try, fact-check and review after changes. If you are not sure, ask for help.
- When you see changes made outside your knowledge, use the current version as your new starting point. Do not blindly overwrite those changes or you suck. Even if you have to update the code, always respect the pattern in the surrounding context!
- Always keep documentation in sync with code. Look at README.md at project root to understand available documentation files.


# Key project requirements

 - Storage must be well tested and durable layer of project. File corruption/error/lost must be impossible.
 - Delivery interfaces must be secure - unauthorized acces must be impossible.
 - ACL must be always checked on backend side, all security checks must be done on backend side. UI/client is not trusted side.


# Programming Best Practices
 
Follow these principles when writing, reviewing, or refactoring code. They can conflict with each other — use judgment over dogma.
 
## Core Principles
 
### KISS — Keep It Simple, Stupid
Favor straightforward solutions over clever ones. Simple code is easier to read, debug, and maintain. If a piece of code feels "clever," treat that as a warning sign and look for a clearer alternative.
 
### YAGNI — You Aren't Gonna Need It
Don't build functionality until it's actually needed. Speculative features add complexity, bugs, and maintenance cost for benefits that often never materialize. Build for today's requirements, not imagined future ones.
 
### DRY — Don't Repeat Yourself
Every piece of knowledge should have a single, authoritative representation in the system. Avoid premature abstraction — tolerate duplication until the pattern is clear (often around the third occurrence). Bad abstraction is worse than duplication.
 
### SOLID
Five object-oriented design principles:
- **Single Responsibility** — a class/module should have one reason to change
- **Open/Closed** — open for extension, closed for modification
- **Liskov Substitution** — subtypes must be substitutable for their base types
- **Interface Segregation** — many specific interfaces beat one general-purpose one
- **Dependency Inversion** — depend on abstractions, not concretions
## Design & Structure
 
### Separation of Concerns
Different aspects of a program (UI, business logic, data access) should live in distinct modules with minimal overlap.
 
### Law of Demeter (Principle of Least Knowledge)
A unit should only talk to its immediate friends. Avoid chains like `a.getB().getC().doSomething()` — they couple code to deep internal structure.
 
### Composition Over Inheritance
Inheritance creates rigid hierarchies and tight coupling. Compose behavior from smaller, focused pieces for more flexibility.
 
### Principle of Least Astonishment
Code should behave the way a reasonable reader expects. Don't surprise people with hidden side effects, unusual naming, or unconventional behavior.
 
## Robustness
 
### Fail Fast
Detect errors as early as possible. Validate inputs at boundaries. Crash loudly on invalid states rather than limping along with corrupted data.
 
### Boy Scout Rule
Leave the code cleaner than you found it. Small, continuous improvements compound over time.
 
## Applying These Principles
 
- Principles conflict: KISS and YAGNI often push back against DRY and SOLID.
- Over-abstracting "just in case" violates YAGNI even when it satisfies DRY.
- Judgment about the specific situation beats rigid rule-following.
- When in doubt, optimize for the next person who has to read the code.
 