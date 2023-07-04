import time
import util


def schedule(source, sink):
    config = {}

    while True:
        request = None
        if not source.empty():
            try:
                request = source.get_nowait()
            except:
                # should not happen
                pass

        tick(request, config, sink)
        time.sleep(1)


def reload_config(request, config):
    now = util.now()
    new_config = {x["url"]: {**x, "next_call": now} for x in request}
    config.update(new_config)
    # TODO proper reloading, leave as is if there is no change
    # TODO validate config, remove items that are beyond interval [5,300]
    # TODO validate regex


def tick(request, config, sink):
    if request:
        reload_config(request, config)
        print(request)

    now = util.now()
    batch = []
    for k, v in config.items():
        if now > v["next_call"]:
            v["next_call"] += v["schedule"]
            batch.append(v)

    if batch:
        sink.put(batch)
