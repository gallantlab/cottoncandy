from io import BytesIO
from pathlib import Path
import time

import cottoncandy as cc

from ..localclient import LocalClient


def test_delete_cleans_up_empty_directories(cci, object_name):
    '''Test that deleting a file also removes empty parent directories (S3-like behavior)'''
    # Create a file in nested directories
    nested_file = object_name + '/subdir1/subdir2/file.txt'
    content = b'test content'

    # Upload the file
    cci.upload_object(nested_file, BytesIO(content))
    time.sleep(cci.wait_time)

    # Verify file exists
    assert cci.exists_object(nested_file) and cci.download_object(nested_file) == content

    # Delete the file
    cci.rm(nested_file)
    time.sleep(cci.wait_time)

    # Verify file is deleted
    assert not cci.exists_object(nested_file)
    assert not cci.exists_object(object_name + '/subdir1/subdir2')
    assert not cci.exists_object(object_name + '/subdir1')
    assert len(cci.glob(object_name)) == 0

    # For local client, verify that empty parent directories are cleaned up on disk
    if isinstance(cci.backend_interface, LocalClient):
        subdir2_path = Path(cci.backend_interface.path) / object_name / 'subdir1' / 'subdir2'
        subdir1_path = Path(cci.backend_interface.path) / object_name / 'subdir1'
        object_path = Path(cci.backend_interface.path) / object_name

        # Verify all empty directories were removed
        assert not subdir2_path.exists(), "subdir2 should be removed after deleting its only file"
        assert not subdir1_path.exists(), "subdir1 should be removed after becoming empty"
        assert not object_path.exists(), "object_name dir should be removed after becoming empty"

    # Try creating objects in place of the deleted directories.
    cci.upload_object(object_name + '/subdir1/subdir2', BytesIO(content))
    time.sleep(cci.wait_time)
    assert cci.exists_object(object_name + '/subdir1/subdir2') \
        and cci.download_object(object_name + '/subdir1/subdir2') == content
    cci.rm(object_name + '/subdir1/subdir2')
    time.sleep(cci.wait_time)

    cci.upload_object(object_name + '/subdir1', BytesIO(content))
    time.sleep(cci.wait_time)
    assert cci.exists_object(object_name + '/subdir1') \
        and cci.download_object(object_name + '/subdir1') == content
    cci.rm(object_name + '/subdir1')
    time.sleep(cci.wait_time)

    # Test recursive=True also cleans up empty directories
    # Create the file again
    cci.upload_object(nested_file, BytesIO(content))
    time.sleep(cci.wait_time)

    # Verify file exists
    assert cci.exists_object(nested_file)

    # Delete a subdir with recursive=True
    cci.rm(object_name + '/subdir1', recursive=True)
    time.sleep(cci.wait_time)

    # Verify file is deleted
    assert not cci.exists_object(nested_file)

    # Verify empty directories are cleaned up (same checks as above)
    if isinstance(cci.backend_interface, LocalClient):
        object_path = Path(cci.backend_interface.path) / object_name
        assert len(list(object_path.glob('**'))) == 0, "object_name dir should be removed after becoming empty"


def test_move_cleans_up_empty_directories(cci, object_name):
    '''Test that moving a file also removes empty parent directories at source (S3-like behavior)'''
    # Create a file in nested directories
    source_file = object_name + '/source/nested/file.txt'
    dest_file = object_name + '/dest/file.txt'
    content = b'test content for move'

    # Upload the file
    cci.upload_object(source_file, BytesIO(content))
    time.sleep(cci.wait_time)

    # Verify file exists
    assert cci.exists_object(source_file)

    # Move the file
    cci.mv(source_file, dest_file, overwrite=True)
    time.sleep(cci.wait_time)

    # Verify file was moved
    assert not cci.exists_object(source_file)
    assert not cci.exists_object(object_name + '/source/nested')
    assert not cci.exists_object(object_name + '/source')
    assert cci.glob(object_name) == [dest_file]

    # For local client, verify that empty parent directories at source are cleaned up on disk
    if isinstance(cci.backend_interface, LocalClient):
        source_nested_path = Path(cci.backend_interface.path) / object_name / 'source' / 'nested'
        source_path = Path(cci.backend_interface.path) / object_name / 'source'

        # Verify empty directories at source were removed
        assert not source_nested_path.exists(), "source/nested should be removed after moving its only file"
        assert not source_path.exists(), "source should be removed after becoming empty"

    # Clean up
    cci.rm(dest_file)
    time.sleep(cci.wait_time)

    # Verify cleanup also works for destination
    if isinstance(cci.backend_interface, LocalClient):
        dest_path = Path(cci.backend_interface.path) / object_name / 'dest'
        object_path = Path(cci.backend_interface.path) / object_name

        assert not dest_path.exists(), "dest dir should be removed after deleting its only file"
        assert not object_path.exists(), "object_name dir should be removed after becoming empty"

    # Try creating objects in place of the deleted directories.
    cci.upload_object(object_name + '/source/nested', BytesIO(content))
    time.sleep(cci.wait_time)
    assert cci.exists_object(object_name + '/source/nested') \
        and cci.download_object(object_name + '/source/nested') == content
    cci.rm(object_name + '/source/nested')
    time.sleep(cci.wait_time)

    cci.upload_object(object_name + '/source', BytesIO(content))
    time.sleep(cci.wait_time)
    assert cci.exists_object(object_name + '/source') \
        and cci.download_object(object_name + '/source') == content
    cci.rm(object_name + '/source')
    time.sleep(cci.wait_time)


def test_move_cleans_up_source_bucket_directories_across_buckets(tmp_path):
    """Moving across buckets should remove empty source directories."""
    source_bucket = tmp_path / 'source-bucket'
    destination_bucket = tmp_path / 'destination-bucket'
    source_bucket.mkdir()
    destination_bucket.mkdir()

    cci_src = cc.get_interface(
        bucket_name=str(source_bucket),
        backend='local',
        verbose=False,
    )
    cci_dest = cc.get_interface(
        bucket_name=str(destination_bucket),
        backend='local',
        verbose=False,
    )
    source_key = 'nested/a/b/file.txt'
    destination_key = 'moved/file.txt'
    content = b'cross bucket move content'

    cci_src.upload_object(source_key, BytesIO(content))

    source_root = source_bucket / 'nested'
    source_a = source_root / 'a'
    source_b = source_a / 'b'
    source_file = source_b / 'file.txt'

    assert source_file.exists()

    # mv from src --> dest, calling from src client
    cci_src.mv(
        source_name=source_key,
        dest_name=destination_key,
        source_bucket=str(source_bucket),
        dest_bucket=str(destination_bucket),
        overwrite=True,
    )

    moved_file = destination_bucket / 'moved' / 'file.txt'
    moved_metadata = destination_bucket / 'moved' / 'file.txt.meta.json'

    assert moved_file.exists()
    assert cci_dest.download_object(destination_key) == content
    assert moved_metadata.exists()
    assert not cci_src.exists_object(source_key, bucket_name=str(source_bucket))

    # The source bucket should be cleaned up back to the bucket root.
    assert not source_file.exists()
    assert not source_b.exists()
    assert not source_a.exists()
    assert not source_root.exists()
    assert source_bucket.exists()

    # Cleanup
    cci_dest.rm(destination_key)
    assert not moved_file.exists()
    assert not moved_metadata.exists()
    assert not (destination_bucket / 'moved').exists()

    # Now try the same thing, but calling `.mv()` from the destination client instead.
    cci_src.upload_object(source_key, BytesIO(content))
    cci_dest.mv(
        source_name=source_key,
        dest_name=destination_key,
        source_bucket=str(source_bucket),
        dest_bucket=str(destination_bucket),
        overwrite=True,
    )

    assert moved_file.exists()
    assert cci_dest.download_object(destination_key) == content
    assert moved_metadata.exists()
    assert not cci_src.exists_object(source_key, bucket_name=str(source_bucket))

    assert not source_file.exists()
    assert not source_b.exists()
    assert not source_a.exists()
    assert not source_root.exists()
    assert source_bucket.exists()
