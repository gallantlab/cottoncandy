from io import BytesIO
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
    assert cci.exists_object(nested_file)

    # Delete the file
    cci.rm(nested_file)
    time.sleep(cci.wait_time)

    # Verify file is deleted
    assert not cci.exists_object(nested_file)

    # For local client, verify that empty parent directories are cleaned up
    if isinstance(cci.backend_interface, LocalClient):
        import os
        subdir2_path = os.path.join(cci.backend_interface.path, object_name, 'subdir1', 'subdir2')
        subdir1_path = os.path.join(cci.backend_interface.path, object_name, 'subdir1')
        object_path = os.path.join(cci.backend_interface.path, object_name)

        # Verify all empty directories were removed
        assert not os.path.exists(subdir2_path), "subdir2 should be removed after deleting its only file"
        assert not os.path.exists(subdir1_path), "subdir1 should be removed after becoming empty"
        assert not os.path.exists(object_path), "object_name dir should be removed after becoming empty"


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
    assert cci.exists_object(dest_file)

    # For local client, verify that empty parent directories at source are cleaned up
    if isinstance(cci.backend_interface, LocalClient):
        import os
        source_nested_path = os.path.join(cci.backend_interface.path, object_name, 'source', 'nested')
        source_path = os.path.join(cci.backend_interface.path, object_name, 'source')

        # Verify empty directories at source were removed
        assert not os.path.exists(source_nested_path), "source/nested should be removed after moving its only file"
        assert not os.path.exists(source_path), "source should be removed after becoming empty"

    # Clean up
    cci.rm(dest_file)
    time.sleep(cci.wait_time)

    # Verify cleanup also works for destination
    if isinstance(cci.backend_interface, LocalClient):
        import os
        dest_path = os.path.join(cci.backend_interface.path, object_name, 'dest')
        object_path = os.path.join(cci.backend_interface.path, object_name)

        assert not os.path.exists(dest_path), "dest dir should be removed after deleting its only file"
        assert not os.path.exists(object_path), "object_name dir should be removed after becoming empty"
