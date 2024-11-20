import json
import logging
import os
import time
from collections import namedtuple
from typing import Any, Mapping

import apache_beam as beam
import requests
from apache_beam import io, pipeline
from apache_beam.io.requestresponse import RequestResponseIO
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
    TestDataflowOptions,
)
from apache_beam.transforms import window
from google.api_core.exceptions import TooManyRequests
from testcontainers.google.pubsub import PubSubContainer

PROJECT = "test-project"
TOPIC = "test-topic"
SUBSCRIPTION = "test-subscription"


logging.basicConfig(
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def setup_pubsub(pubsub):
    pub = pubsub.get_publisher_client()
    sub = pubsub.get_subscriber_client()
    topic_path = pub.topic_path(PROJECT, TOPIC)
    pub.create_topic(request={"name": pub.topic_path(PROJECT, TOPIC)})
    logging.info("created topic")
    logging.info(os.environ.get("PUBSUB_EMULATOR_HOST"))

    with sub:
        sub.create_subscription(
            request={
                "name": sub.subscription_path(PROJECT, SUBSCRIPTION),
                "topic": topic_path,
            }
        )
    logging.info("created subscription")
    return pub, sub


PokeTrainer = namedtuple("PokeTrainer", ["id", "name", "pokemons"])
PokeMon = namedtuple("PokeMon", ["name", "weight", "height"])


class Deserialize(beam.DoFn):
    def process(self, element: bytes):
        yield PokeTrainer(**json.loads(element.decode()))


class Debug(beam.DoFn):
    def process(self, element):
        logging.info(element)
        yield element


class CreateKey(beam.DoFn):
    def process(self, element: PokeTrainer):
        yield (element.id, element)


class ExtractPokemons(beam.DoFn):
    def process(self, element):
        for pokemon in element.pokemons:
            yield (pokemon, element.id)


class Explode(beam.DoFn):
    def process(self, element):
        ids, pokemon = element
        for id in ids:
            yield (id, pokemon)


class CombineTrainerAndPokemons(beam.DoFn):
    def process(self, element):
        data = element[1]
        trainer = data["trainer"][0]
        yield PokeTrainer(trainer.id, trainer.name, data["pokemons"])


class PokeApi:
    def __call__(self, element: tuple[str, list[int]]):
        pokemon, ids = element
        logging.info("calling pokeapi for %s", pokemon)
        response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{pokemon}")
        if response.status_code == 409:
            raise TooManyRequests("too many requests")
        if response.status_code != 200:
            raise Exception(f"failed to get data for {pokemon}")
        data = response.json()
        return (
            ids,
            PokeMon(pokemon, data["weight"], data["height"]),
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return None

    def get_cache_key(self, request) -> str:
        """Returns the request to be cached.

        This is how the response will be looked up in the cache as well.
        By default, entire request is cached as the key for the cache.
        Implement this method to override the key for the cache.
        For example, in `BigTableEnrichmentHandler`, the row key for the element
        is returned here.
        """
        return ""

    def batch_elements_kwargs(self) -> Mapping[str, Any]:
        """Returns a kwargs suitable for `beam.BatchElements`."""
        return {}


if __name__ == "__main__":
    with PubSubContainer() as pubsub:
        os.environ["PUBSUB_EMULATOR_HOST"] = pubsub.get_pubsub_emulator_host()
        pub, sub = setup_pubsub(pubsub)
        subscription = sub.subscription_path(PROJECT, SUBSCRIPTION)
        input_data = [
            {"id": 1, "name": "Ash Ketchum", "pokemons": ["pikachu", "charizard"]},
            {"id": 2, "name": "Misty", "pokemons": ["charizard", "squirtle"]},
            {"id": 3, "name": "Mike", "pokemons": ["pikachu", "charizard", "squirtle"]},
        ]
        for item in input_data:
            pub.publish(
                topic=pub.topic_path(PROJECT, TOPIC),
                data=json.dumps(item).encode(),
            )

        options = PipelineOptions()
        options.view_as(StandardOptions).streaming = True
        options.view_as(GoogleCloudOptions).no_auth = True
        options.view_as(
            TestDataflowOptions
        ).pubsubRootUrl = pubsub.get_pubsub_emulator_host()

        p = pipeline.Pipeline(options=options)
        trainers = (
            p
            | "Read Events" >> io.ReadFromPubSub(subscription=subscription)
            | "Deserialize" >> beam.ParDo(Deserialize())
            | "Create Window" >> beam.WindowInto(window.FixedWindows(10))
            | "Debug1" >> beam.ParDo(Debug())
        )
        keyed_trainers = (
            trainers | "Create Key" >> beam.ParDo(CreateKey())
            # | "Debug2" >> beam.ParDo(Debug())
        )
        keyed_pokemons = (
            trainers
            | "Extract Pokemons" >> beam.ParDo(ExtractPokemons())
            | "Group By Pokemon" >> beam.GroupByKey()
            | "Debug4" >> beam.ParDo(Debug())
            | "Get Pokemon Data"
            >> RequestResponseIO(
                caller=PokeApi(),
            )
            | "Explode" >> beam.ParDo(Explode())
            # | "Debug5" >> beam.ParDo(Debug())
        )
        joined = (
            ({"trainer": keyed_trainers, "pokemons": keyed_pokemons})
            | "Join" >> beam.CoGroupByKey()
            | "Combine Trainer and Pokemons" >> beam.ParDo(CombineTrainerAndPokemons())
            | "Debug6" >> beam.ParDo(Debug())
        )

        result = p.run()

        for tick in range(60):
            time.sleep(1)
            if tick == 10:
                pub.publish(
                    topic=pub.topic_path(PROJECT, TOPIC),
                    data=json.dumps(
                        {"id": 4, "name": "Rocko", "pokemons": ["onix"]},
                    ).encode(),
                )

        result.cancel()
