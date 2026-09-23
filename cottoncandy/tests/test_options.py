"""Tests for the configuration profile resolution in ``cottoncandy.options``.

These tests build an in-memory ``ConfigParser`` and resolve profiles against it,
so they do not touch the user's real configuration file or any network backend.
"""
import configparser

import pytest

import cottoncandy as cc
import cottoncandy.interfaces
from cottoncandy import options


def make_config():
    """Base sections plus a handful of profiles used across the tests."""
    cfg = configparser.ConfigParser()
    cfg.read_dict({
        'login': {
            'access_key': 'BASEACCESS',
            'secret_key': 'BASESECRET',
            'endpoint_url': 'https://s3.amazonaws.com/',
        },
        'basic': {
            'default_bucket': 'base-bucket',
            'signature_version': '',
            'force_bucket_creation': 'False',
            'backend': 's3',
        },
        'gdrive': {
            'secrets': 'client_secrets.json',
            'credentials': 'credentials.txt',
        },
        'profile:lab': {
            'access_key': 'LABACCESS',
            'secret_key': 'LABSECRET',
            'endpoint_url': 'https://s3.example.edu/',
            'default_bucket': 'lab-shared',
        },
        'profile:lab-scratch': {
            'inherits': 'lab',
            'default_bucket': 'lab-scratch',
        },
        'profile:lab-scratch-ro': {
            'inherits': 'lab-scratch',
            'signature_version': 's3v4',
        },
        'profile:drive': {
            'backend': 'gdrive',
            'secrets': 'lab_secrets.json',
            'credentials': 'lab_creds.txt',
        },
        'profile:cycle-a': {'inherits': 'cycle-b'},
        'profile:cycle-b': {'inherits': 'cycle-a'},
    })
    return cfg


def test_default_profile_uses_base_sections():
    cfg = make_config()
    settings = options.get_profile(None, cfg=cfg)
    assert settings['access_key'] == 'BASEACCESS'
    assert settings['secret_key'] == 'BASESECRET'
    assert settings['endpoint_url'] == 'https://s3.amazonaws.com/'
    assert settings['default_bucket'] == 'base-bucket'
    assert settings['backend'] == 's3'


def test_profile_overrides_only_specified_keys():
    cfg = make_config()
    settings = options.get_profile('lab', cfg=cfg)
    # set by the profile
    assert settings['access_key'] == 'LABACCESS'
    assert settings['endpoint_url'] == 'https://s3.example.edu/'
    assert settings['default_bucket'] == 'lab-shared'
    # not set by the profile -> falls back to the base sections
    assert settings['backend'] == 's3'
    assert settings['force_bucket_creation'] == 'False'


def test_inheritance_single_level():
    cfg = make_config()
    settings = options.get_profile('lab-scratch', cfg=cfg)
    # inherited from the parent profile (lab)
    assert settings['access_key'] == 'LABACCESS'
    assert settings['endpoint_url'] == 'https://s3.example.edu/'
    # overridden by the child
    assert settings['default_bucket'] == 'lab-scratch'


def test_inheritance_multi_level():
    cfg = make_config()
    settings = options.get_profile('lab-scratch-ro', cfg=cfg)
    assert settings['access_key'] == 'LABACCESS'        # from lab
    assert settings['default_bucket'] == 'lab-scratch'  # from lab-scratch
    assert settings['signature_version'] == 's3v4'      # own


def test_gdrive_profile():
    cfg = make_config()
    settings = options.get_profile('drive', cfg=cfg)
    assert settings['backend'] == 'gdrive'
    assert settings['secrets'] == 'lab_secrets.json'
    assert settings['credentials'] == 'lab_creds.txt'


def test_unknown_profile_raises():
    cfg = make_config()
    with pytest.raises(ValueError):
        options.get_profile('does-not-exist', cfg=cfg)


def test_circular_inheritance_raises():
    cfg = make_config()
    with pytest.raises(ValueError):
        options.get_profile('cycle-a', cfg=cfg)


def test_list_profiles():
    cfg = make_config()
    profiles = set(options.list_profiles(cfg=cfg))
    assert {'lab', 'lab-scratch', 'lab-scratch-ro', 'drive'} <= profiles
    # base sections are not profiles
    assert 'login' not in profiles
    assert 'basic' not in profiles


class _StubInterface:
    """Records the arguments ``get_interface`` passes to ``DefaultInterface``."""

    def __init__(self, bucket_name, access_key, secret_key, endpoint_url,
                 force_bucket_creation, verbose=True, backend='s3', **kwargs):
        self.bucket_name = bucket_name
        self.access_key = access_key
        self.secret_key = secret_key
        self.endpoint_url = endpoint_url
        self.force_bucket_creation = force_bucket_creation
        self.backend = backend
        self.kwargs = kwargs


def test_get_interface_uses_profile(monkeypatch):
    cfg = make_config()
    monkeypatch.setattr(options, 'config', cfg)
    monkeypatch.setattr(cottoncandy.interfaces, 'DefaultInterface',
                        _StubInterface)
    cci = cc.get_interface(profile='lab')
    assert cci.bucket_name == 'lab-shared'
    assert cci.access_key == 'LABACCESS'
    assert cci.secret_key == 'LABSECRET'
    assert cci.endpoint_url == 'https://s3.example.edu/'
    assert cci.backend == 's3'


def test_get_interface_explicit_arg_overrides_profile(monkeypatch):
    cfg = make_config()
    monkeypatch.setattr(options, 'config', cfg)
    monkeypatch.setattr(cottoncandy.interfaces, 'DefaultInterface',
                        _StubInterface)
    cci = cc.get_interface(bucket_name='override-bucket', profile='lab')
    assert cci.bucket_name == 'override-bucket'
    # the remaining settings still come from the profile
    assert cci.access_key == 'LABACCESS'
    assert cci.endpoint_url == 'https://s3.example.edu/'
