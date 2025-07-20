from ftm_lakehouse.crawl import crawl


def test_process_update(tmp_dataset, fixtures_path):
    crawl(fixtures_path / "src", tmp_dataset)
    tmp_dataset.make()
