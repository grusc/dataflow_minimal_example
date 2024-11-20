import json
import logging
import os
import time
from collections import namedtuple
from typing import Any

import apache_beam as beam

from apache_beam.io import fileio
from apache_beam import io, pipeline, pvalue
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions,
    PipelineOptions,
    StandardOptions,
    TestDataflowOptions,
)
from testcontainers.google.pubsub import PubSubContainer

PROJECT = "test-project"
TOPIC = "test-topic"
SUBSCRIPTION = "test-subscription"
SIDE_INPUT_FILE = "pokemons.json"


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

    with sub:
        sub.create_subscription(
            request={
                "name": sub.subscription_path(PROJECT, SUBSCRIPTION),
                "topic": topic_path,
            }
        )
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


class Lookup(beam.DoFn):
    def process(self, element: PokeTrainer, side_input: dict[str, dict[str, Any]]):
        pokemons = [side_input.get(p) for p in element.pokemons]
        yield PokeTrainer(
            element.id, element.name, [p for p in pokemons if p is not None]
        )


if __name__ == "__main__":
    with PubSubContainer() as pubsub:
        os.environ["PUBSUB_EMULATOR_HOST"] = pubsub.get_pubsub_emulator_host()
        pub, sub = setup_pubsub(pubsub)
        subscription = sub.subscription_path(PROJECT, SUBSCRIPTION)
        with open(SIDE_INPUT_FILE, "w") as file:
            json.dump(
                {
                    "pikachu": {"name": "pikachu", "weight": 1, "height": 1},
                    "charizard": {"name": "charizard", "weight": 2, "height": 2},
                },
                file,
            )
        input_data = [
            {"id": 1, "name": "Ash Ketchum", "pokemons": ["pikachu", "charizard"]},
            {"id": 3, "name": "Mike", "pokemons": ["charizard"]},
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
        pokemons = (
            p
            | fileio.MatchFiles(SIDE_INPUT_FILE)
            | fileio.ReadMatches()
            | beam.Map(lambda f: (json.loads(f.read_utf8())))
            | "Debug2" >> beam.ParDo(Debug())
        )
        trainers = (
            p
            | "Read Events" >> io.ReadFromPubSub(subscription=subscription)
            | "Deserialize" >> beam.ParDo(Deserialize())
            | "Lookup" >> beam.ParDo(Lookup(), side_input=pvalue.AsSingleton(pokemons))
            | "Debug1" >> beam.ParDo(Debug())
        )
        result = p.run()
        for tick in range(1, 61):
            time.sleep(1)
            if tick == 30:
                with open(SIDE_INPUT_FILE, "w") as file:
                    json.dump(
                        {
                            "onix": {"name": "onix", "weight": 3, "height": 3},
                        },
                        file,
                    )
                pub.publish(
                    topic=pub.topic_path(PROJECT, TOPIC),
                    data=json.dumps(
                        {"id": 2, "name": "Rocko", "pokemons": ["onix"]},
                    ).encode(),
                )
        logging.info("shutting down")
        result.cancel()
