import argparse
import logging

from .datacatalog_fileset_enricher import DatacatalogFilesetEnricher


class DatacatalogFilesetEnricherCLI:

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()
        cls._parse_args(argv)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(description=__doc__,
                                         formatter_class=argparse.RawDescriptionHelpFormatter)

        parser.add_argument('--project-id', help='Project id', required=True)

        parser.set_defaults(func=lambda *inner_args: logging.info('Must use a subcommand'))

        subparsers = parser.add_subparsers()

        enrich_filesets = subparsers.add_parser('enrich-gcs-filesets',
                                                help='Enrich filesets with Tags')

        enrich_filesets.add_argument('--tag-template-name',
                                     help='Name of the Fileset Enrich template,'
                                     'i.e: '
                                     'projects/my-project/locations/us-central1/tagTemplates/'
                                     'my_template_test')

        enrich_filesets.add_argument('--entry-group-id', help='Entry Group ID')
        enrich_filesets.add_argument('--entry-id', help='Entry ID')
        enrich_filesets.add_argument('--tag-fields',
                                     help='Specify the fields you want on the generated Tags,'
                                     ' split by comma, use the list available in the docs')
        enrich_filesets.add_argument('--bucket-prefix',
                                     help='Specify a bucket prefix if you want to avoid scanning'
                                     ' too many GCS buckets')
        enrich_filesets.set_defaults(func=cls.__enrich_fileset)

        create_template = subparsers.add_parser('create-template',
                                                help='Create the Fileset Enrich template')

        create_template.add_argument('--location',
                                     help='Location of the Fileset Enrich template',
                                     default='us-central1')

        create_template.set_defaults(func=cls.__create_fileset_template)

        clean_up_tags = subparsers.add_parser(
            'clean-up-templates-and-tags',
            help='Clean up the Fileset Enhancer Template and Tags From the Fileset Entries')
        clean_up_tags.set_defaults(func=cls.__clean_up_fileset_template_and_tags)

        clean_up_all = subparsers.add_parser(
            'clean-up-all',
            help='Clean up Fileset Entries, Their Tags and the Fileset Enhancer Template')
        clean_up_all.set_defaults(func=cls.__clean_up_all)

        args = parser.parse_args(argv)
        args.func(args)

    @classmethod
    def __enrich_fileset(cls, args):
        tag_fields = None
        if args.tag_fields:
            tag_fields = args.tag_fields.split(',')

        DatacatalogFilesetEnricher(args.project_id).run(args.entry_group_id, args.entry_id,
                                                        tag_fields, args.bucket_prefix,
                                                        args.tag_template_name)

    @classmethod
    def __clean_up_fileset_template_and_tags(cls, args):
        DatacatalogFilesetEnricher(args.project_id).clean_up_fileset_template_and_tags()

    @classmethod
    def __clean_up_all(cls, args):
        DatacatalogFilesetEnricher(args.project_id).clean_up_all()

    @classmethod
    def __create_fileset_template(cls, args):
        DatacatalogFilesetEnricher(args.project_id).create_template(args.location)
