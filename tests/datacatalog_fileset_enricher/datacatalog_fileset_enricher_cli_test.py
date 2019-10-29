from unittest import TestCase

from datacatalog_fileset_enricher import datacatalog_fileset_enricher_cli


class TagManagerCLITest(TestCase):
    def test_parse_args_invalid_subcommand_should_raise_system_exit(self):
        self.assertRaises(SystemExit, datacatalog_fileset_enricher_cli.
                          DatacatalogFilesetEnricherCLI._parse_args,
                          ['invalid-subcommand'])

    def test_parse_args_enrich_gcs_filesets_missing_mandatory_args_should_raise_system_exit(self):
        self.assertRaises(SystemExit, datacatalog_fileset_enricher_cli.
                          DatacatalogFilesetEnricherCLI._parse_args,
                          ['enrich-gcs-filesets'])

    def test_run_no_args_should_raise_system_exit(self):
        self.assertRaises(SystemExit, datacatalog_fileset_enricher_cli.
                          DatacatalogFilesetEnricherCLI.run, None)
