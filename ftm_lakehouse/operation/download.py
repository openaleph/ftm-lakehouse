"""Download operation - export the archive files to their nice paths."""

from anystore.logic.constants import CHUNK_SIZE_LARGE
from anystore.logic.io import stream
from anystore.store import get_store
from anystore.types import Uri

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.job import DatasetJobModel
from ftm_lakehouse.operation.base import DatasetJobOperation
from ftm_lakehouse.repository.job import JobRun


class DownloadArchiveJob(DatasetJobModel):
    target: Uri
    skipped: int = 0


class DownloadArchiveOperation(DatasetJobOperation[DownloadArchiveJob]):
    """
    Download the archive files to a target transforming into nice paths based on
    exported documents.csv
    """

    target = tag.OP_DOWNLOAD_ARCHIVE
    dependencies = [path.EXPORTS_DOCUMENTS]

    def handle(self, run: JobRun[DownloadArchiveJob], *args, **kwargs) -> None:
        target = get_store(run.job.target)
        self.log.info(
            "Downloading archive ...",
            target=target.uri,
            documents=self.documents.csv_uri,
        )
        for document in self.documents.stream():
            if target.exists(document.relative_path):
                self.log.debug(
                    f"Skipping `{document.relative_path}`, already exists.",
                    checksum=document.checksum,
                    source=self.archive.uri,
                    target=target.uri,
                )
                run.job.skipped += 1
                continue

            self.log.info(
                f"Downloading `{document.relative_path}` ...",
                checksum=document.checksum,
                source=self.archive.uri,
                target=target.uri,
            )
            with target.open(document.relative_path, "wb") as o:
                with self.archive.open(document.checksum) as i:
                    stream(i, o, CHUNK_SIZE_LARGE)
            run.job.done += 1
