"""Mixedbread Parsing Haystack converter module."""

from abc import ABC, abstractmethod
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

from haystack.utils import Secret, deserialize_secrets_inplace
from haystack import Document, component, default_from_dict, default_to_dict, logging
from haystack.components.converters.utils import normalize_metadata

from mixedbread import Mixedbread
from tqdm import tqdm

MIXEDBREAD_API_URL = "https://api.mixedbread.com"


@component
class MixedbreadFileConverter:
    """Mixedbread Parsing Haystack converter."""

    def __init__(
        self,
        api_url: str = MIXEDBREAD_API_URL,
        api_key: Optional[Union[Secret, str]] = Secret.from_env_var(
            "MIXEDBREAD_API_KEY", strict=False
        ),
    ):
        """
        :param api_url: The Mixedbread API URL.
        :param api_key: The Mixedbread API key.
        """
        self._api_url = api_url
        self._api_key = api_key

        is_hosted_api = api_url == MIXEDBREAD_API_URL
        api_key_value = (
            api_key.resolve_value() if isinstance(api_key, Secret) else api_key
        )

        if is_hosted_api and not api_key_value:
            msg = (
                "To use the hosted version of Mixedbread, you need to set the environment variable "
                "MIXEDBREAD_API_KEY (recommended) or explicitly pass the parameter api_key."
            )
            raise ValueError(msg)

        self.clinet = Mixedbread(base_url=api_url, api_key=api_key_value)

    @component.output_types(documents=list[Document])
    def run(
        self,
        paths: Iterable[Union[Path, str]],
        meta: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        show_progress: bool = True,
    ):
        """Convert different file types (PDF, Word, Powerpoint) into Markdown. Chunking is on page level.
        Each page is a separate document. The document id is the file id and the chunk id is the page number.
        The metadata contains the bboxes for each element in the page.

        :param paths: List of paths to convert. Paths can be files or directories.
            If a path is a directory, all files in the directory are converted. Subdirectories are ignored.

        :param meta: Optional metadata to attach to the Documents.
            This value can be either a list of dictionaries or a single dictionary.
            If it's a single dictionary, its content is added to the metadata of all produced Documents.
            If it's a list, the length of the list must match the number of paths, because the two lists will be zipped.
            Please note that if the paths contain directories, `meta` can only be a single dictionary
            (same metadata for all files).

        :param show_progress: Whether to show a progress bar.

        :returns: A dictionary with the following key:
            - `documents`: List of Haystack Documents.

        :raises ValueError: If `meta` is a list and `paths` contains directories.
        """
        paths_obj = [Path(path) for path in paths]
        filepaths = [path for path in paths_obj if path.is_file()]
        filepaths_in_directories = [
            filepath
            for path in paths_obj
            if path.is_dir()
            for filepath in path.glob("*.*")
            if filepath.is_file()
        ]

        if filepaths_in_directories and isinstance(meta, list):
            error = """"If providing directories in the `paths` parameter,
             `meta` can only be a dictionary (metadata applied to every file),
             and not a list. To specify different metadata for each file,
             provide an explicit list of direct paths instead."""
            raise ValueError(error)

        all_filepaths = filepaths + filepaths_in_directories
        documents: list[Document] = []

        meta_list = normalize_metadata(meta, sources_count=len(all_filepaths))

        for filepath, metadata in tqdm(
            zip(all_filepaths, meta_list),
            desc="Converting files to Haystack Documents",
            disable=not show_progress,
        ):
            result_document = self.clinet.parsing.jobs.upload_and_poll(
                file=filepath,
                return_format="markdown",
            )

            for i, chunk in enumerate(result_document.result.chunks):
                text = ""
                bboxes = []
                for element in chunk.elements:
                    if element.type in ["picture"]:
                        text += f"Image Summary: {element.summary}\n"
                    else:
                        text += f"{element.content}\n"
                    bboxes.append(
                        {
                            "type": element.type,
                            "bbox": element.bbox,
                            "page": element.page,
                        }
                    )

                metadata["_bboxes"] = bboxes

                documents.append(
                    Document(
                        id=f"{result_document.file_id}_{i}",
                        content=text,
                        meta=metadata,
                    )
                )

        return {"documents": documents}
