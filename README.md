# Setup
install dependencies via
```
pip install -r requirements.txt
```
Make sure docker is running bevor executing any code.

# Examples
## Enhancement with window
In this example we try to enrich data via an http call to an api.
As we do not want to make retundant calls to the api we group data by individual pokemons and make only one call by pokemon.
This works fine, however there is are some question marks around the window function.
Running the script windows_example.py yields the following log on my machine:
```
2024-11-21 09:10:49 INFO     Running pipeline with DirectRunner.
2024-11-21 09:10:49 INFO     PokeTrainer(id=1, name='Ash Ketchum', pokemons=['pikachu', 'charizard'])
2024-11-21 09:10:49 INFO     PokeTrainer(id=2, name='Misty', pokemons=['charizard', 'squirtle'])
2024-11-21 09:10:49 INFO     PokeTrainer(id=3, name='Mike', pokemons=['pikachu', 'charizard', 'squirtle'])
2024-11-21 09:11:01 INFO     ('pikachu', [1, 3])
2024-11-21 09:11:01 INFO     ('charizard', [1, 2, 3])
2024-11-21 09:11:01 INFO     ('squirtle', [2, 3])
2024-11-21 09:11:01 INFO     PokeTrainer(id=4, name='Rocko', pokemons=['onix']) <<<<<
2024-11-21 09:11:01 INFO     calling pokeapi for pikachu
2024-11-21 09:11:01 INFO     calling pokeapi for charizard
2024-11-21 09:11:01 INFO     calling pokeapi for squirtle
2024-11-21 09:11:02 INFO     PokeTrainer(id=1, name='Ash Ketchum', pokemons=[PokeMon(name='pikachu', weight=60, height=4), PokeMon(name='charizard', weight=905, height=17)])
2024-11-21 09:11:02 INFO     PokeTrainer(id=2, name='Misty', pokemons=[PokeMon(name='charizard', weight=905, height=17), PokeMon(name='squirtle', weight=90, height=5)])
2024-11-21 09:11:02 INFO     PokeTrainer(id=3, name='Mike', pokemons=[PokeMon(name='pikachu', weight=60, height=4), PokeMon(name='charizard', weight=905, height=17), PokeMon(name='squirtle', weight=90, height=5)])
2024-11-21 09:11:31 INFO     ('onix', [4]) <<<<<
2024-11-21 09:11:31 INFO     calling pokeapi for onix
2024-11-21 09:11:31 INFO     PokeTrainer(id=4, name='Rocko', pokemons=[PokeMon(name='onix', weight=2100, height=88)])
```
As one can see for the first window with the first three trainers arriving @ 09:10:49 the enrichment starts after the 10 sec window @ 09:11:01.
However for the next trainer, arriving @ 09:11:01 I would expect the next window to fire also around 10 sec later. But in this case, the enrichment only starts after 30 seconds, which is the equivalent of 3 window sizes.
How can this behavior be explained?

## Enhacement with side input
For the sideput example we have basically the same use case but instead of calling the api we use a lookup file.
Running the script side_input_example.py yields the following log:
```
2024-11-21 09:18:42 INFO     Running pipeline with DirectRunner.
2024-11-21 09:18:42 INFO     {'pikachu': {'name': 'pikachu', 'weight': 1, 'height': 1}, 'charizard': {'name': 'charizard', 'weight': 2, 'height': 2}}
2024-11-21 09:18:42 INFO     PokeTrainer(id=1, name='Ash Ketchum', pokemons=[{'name': 'pikachu', 'weight': 1, 'height': 1}, {'name': 'charizard', 'weight': 2, 'height': 2}])
2024-11-21 09:18:42 INFO     PokeTrainer(id=3, name='Mike', pokemons=[{'name': 'charizard', 'weight': 2, 'height': 2}])
2024-11-21 09:19:12 INFO     PokeTrainer(id=2, name='Rocko', pokemons=[])
```
As one can see the last enrichment fails because the file containing the relevant information is only updated to include the required data after the start of the pipeline.
What would be the best solution to update the lookup collection after the start of the pipeline?

# Other Questions
## naive in memory cache in java
We have implemented a naive in memory cache like in naive_in_memory_cache.java for one of our pipelines.
Here the simple question is: considering beams execution model what is the best practice for developing such an in memory cache in the java sdk?
Are there some things we did not consider? Did we shoot ourselves in the foot with that one, because it will come back to haunt us?
