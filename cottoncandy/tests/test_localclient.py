from io import BytesIO
from pathlib import Path
import time

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
    assert cci.exists_object(object_name + '/subdir1/subdir2') \
        and cci.download_object(object_name + '/subdir1/subdir2') == content
    cci.rm(object_name + '/subdir1/subdir2')

    cci.upload_object(object_name + '/subdir1', BytesIO(content))
    assert cci.exists_object(object_name + '/subdir1') \
        and cci.download_object(object_name + '/subdir1') == content
    cci.rm(object_name + '/subdir1')


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
    assert cci.exists_object(dest_file)

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
    assert cci.exists_object(object_name + '/source/nested') \
        and cci.download_object(object_name + '/source/nested') == content
    cci.rm(object_name + '/source/nested')

    cci.upload_object(object_name + '/source', BytesIO(content))
    assert cci.exists_object(object_name + '/source') \
        and cci.download_object(object_name + '/source') == content
    cci.rm(object_name + '/source')
