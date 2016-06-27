from routes.mapper import SubMapper
import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit

import ckanext.s3filestore.uploader

import boto
import logging
import pylons
config = pylons.config
log = logging.getLogger(__name__)

p_key = config.get('ckanext.s3filestore.aws_access_key_id')
s_key = config.get('ckanext.s3filestore.aws_secret_access_key')
bucket_name = config.get('ckanext.s3filestore.aws_bucket_name')

class S3FileStorePlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IConfigurable)
    plugins.implements(plugins.IUploader)
    plugins.implements(plugins.IRoutes, inherit=True)
    plugins.implements(plugins.IResourceController, inherit=True)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')

    # IConfigurable

    def configure(self, config):
        # Certain config options must exists for the plugin to work. Raise an
        # exception if they're missing.
        missing_config = "{0} is not configured. Please amend your .ini file."
        config_options = (
            'ckanext.s3filestore.aws_access_key_id',
            'ckanext.s3filestore.aws_secret_access_key',
            'ckanext.s3filestore.aws_bucket_name'
        )
        for option in config_options:
            if not config.get(option, None):
                raise RuntimeError(missing_config.format(option))

        # Check that options actually work, if not exceptions will be raised
        if toolkit.asbool(
                config.get('ckanext.s3filestore.check_access_on_startup',
                           True)):
            ckanext.s3filestore.uploader.BaseS3Uploader().get_s3_bucket(
                config.get('ckanext.s3filestore.aws_bucket_name'))

    # IUploader

    def get_resource_uploader(self, data_dict):
        '''Return an uploader object used to upload resource files.'''
        return ckanext.s3filestore.uploader.S3ResourceUploader(data_dict)

    def get_uploader(self, upload_to, old_filename=None):
        '''Return an uploader object used to upload general files.'''
        return ckanext.s3filestore.uploader.S3Uploader(upload_to,
                                                       old_filename)

    # IRoutes

    def before_map(self, map):
        with SubMapper(map, controller='ckanext.s3filestore.controller:S3Controller') as m:
            # Override the resource download links
            m.connect('resource_download',
                      '/dataset/{id}/resource/{resource_id}/download',
                      action='resource_download')
            m.connect('resource_download',
                      '/dataset/{id}/resource/{resource_id}/download/{filename}',
                      action='resource_download')

            # fallback controller action to download from the filesystem
            m.connect('filesystem_resource_download',
                      '/dataset/{id}/resource/{resource_id}/fs_download/{filename}',
                      action='filesystem_resource_download')

            # Intercept the uploaded file links (e.g. group images)
            m.connect('uploaded_file', '/uploads/{upload_to}/{filename}',
                      action='uploaded_file_redirect')

        return map

    #IResourceController

    def before_delete(self, context, resource, resources):
        S3_conn = boto.connect_s3(p_key, s_key)
        bucket = S3_conn.get_bucket(bucket_name)
        for key in bucket.list():
            if resource['id'] in key.name:
                log.debug('Delete %s', key.name)
                bucket.delete_key(key.name)

    def after_update(self, context, resource):
        S3_conn = boto.connect_s3(p_key, s_key)
        bucket = S3_conn.get_bucket(bucket_name)
        for key in bucket.list():
            if resource['id'] in key.name:
                if resource['url'].rfind('/'):
                    file_name = resource['url'][resource['url'].rfind('/')+1:]
                else:
                    file_name = resource['url']
                if file_name not in key.name:
                    log.debug('Delete old %s after Update %s', key.name, file_name)
                    bucket.delete_key(key.name)
