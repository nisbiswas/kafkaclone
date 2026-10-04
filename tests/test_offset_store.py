from broker.offset_store import OffsetStore


def test_offset_store_commit_and_get(tmp_path):

    store=OffsetStore(tmp_path)

    offset=store.get("group1","orders",0)

    assert offset==0

    store.commit("group1","orders",0,5)

    offset=store.get("group1","orders",0)
    assert offset==5