from unittest import TestCase
from unittest import mock

from datacatalog_fileset_enricher import datacatalog_fileset_enricher_cli


class TagManagerCLITest(TestCase):
    __PATCHED_FILE_ENRICHER_PROCESSOR = 'datacatalog_fileset_enricher' \
                                         '.datacatalog_fileset_enricher.DatacatalogFilesetEnricher'

    def test_parse_args_invalid_subcommand_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI._parse_args,
            ['invalid-subcommand'])

    def test_parse_args_enrich_gcs_filesets_missing_mandatory_args_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI._parse_args,
            ['enrich-gcs-filesets'])

    def test_run_no_args_should_raise_system_exit(self):
        self.assertRaises(SystemExit,
                          datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI.run, None)

    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.__init__', lambda self, *args: None)
    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.run')
    def test_run_with_args_should_not_raise_exception(self, run):
        datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI.run(
            ['--project-id=test-project', 'enrich-gcs-filesets'])
        run.assert_called_once()

    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.__init__', lambda self, *args: None)
    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.run')
    def test_run_with_args_and_tag_fields_should_not_raise_exception(self, run):
        datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI.run(
            ['--project-id=test-project', 'enrich-gcs-filesets', '--tag-fields=field1,field2'])
        run.assert_called_once()

    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.__init__', lambda self, *args: None)
    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.clean_up_fileset_template_and_tags')
    def test_clen_up_fileset_templates_and_tag_with_args_should_not_raise_exception(
        self, clean_up_fileset_template_and_tags):
        datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI.run(
            ['--project-id=test-project', 'clean-up-templates-and-tags'])
        clean_up_fileset_template_and_tags.assert_called_once()

    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.__init__', lambda self, *args: None)
    @mock.patch(f'{__PATCHED_FILE_ENRICHER_PROCESSOR}.clean_up_all')
    def test_clen_up_all_with_args_should_not_raise_exception(self, clean_up_all):
        datacatalog_fileset_enricher_cli.DatacatalogFilesetEnricherCLI.run(
            ['--project-id=test-project', 'clean-up-all'])
        clean_up_all.assert_called_once()
