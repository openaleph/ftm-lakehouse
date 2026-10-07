from ftm_lakehouse.logic.entities.aggregate import aggregate_unsafe


def _caption(props: dict[str, str]) -> str:
    rows = [
        {
            "entity_id": "jane",
            "schema": "Person",
            "dataset": "test",
            "prop": p,
            "value": v,
        }
        for p, v in props.items()
    ]
    (payload,) = aggregate_unsafe(iter(rows), "test")
    return payload.to_dict()["caption"]


def test_caption_first_caption_prop():
    # `email` comes after `name` in `Person.caption`
    assert _caption({"name": "Jane Doe", "email": "jane@example.org"}) == "Jane Doe"
    assert _caption({"email": "jane@example.org"}) == "jane@example.org"
    assert _caption({"nationality": "de"}) == "Person"
