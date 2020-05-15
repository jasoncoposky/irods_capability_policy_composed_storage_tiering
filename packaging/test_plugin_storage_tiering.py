import os
import sys
import shutil
import contextlib
import tempfile
import json
import os.path

from time import sleep

if sys.version_info >= (2, 7):
    import unittest
else:
    import unittest2 as unittest

from ..configuration import IrodsConfig
from ..controller import IrodsController
from .resource_suite import ResourceBase
from ..test.command import assert_command
from . import session
from .. import test
from .. import paths
from .. import lib
import ustrings

def insert_plugins(irods_config, without=""):
    irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
        {
            "instance_name": "irods_rule_engine_plugin-event_handler-data_object_modified-instance",
            "plugin_name": "irods_rule_engine_plugin-event_handler-data_object_modified",
            "plugin_specific_configuration": {
                "policies_to_invoke" : [
                    {
                        "active_policy_clauses" : ["post"],
                        "events" : ["put", "get", "create", "read", "write", "rename", "register", "unregister", "replication", "checksum", "copy", "seek", "truncate"],
			 "policy" : "irods_policy_access_time",
			 "configuration" : {
			    "log_errors" : "true"
			 }
                    },

                    {
                        "active_policy_clauses" : ["post"],
                        "events" : ["read", "write", "get"],
                        "policy"    : "irods_policy_data_restage",
                        "configuration" : {
                        }
                    },

                    {
                        "active_policy_clauses" : ["post"],
                        "events" : ["replication"],
			    "policy"    : "irods_policy_tier_group_metadata",
			    "configuration" : {
			    }

                    },

                    {
                        "active_policy_clauses" : ["post"],
                        "events" : ["replication"],
			    "policy"    : "irods_policy_data_verification",
			    "configuration" : {
			    }

                    },

                    {
                        "active_policy_clauses" : ["post"],
                        "events" : ["replication"],
			    "policy"    : "irods_policy_data_retention",
			    "configuration" : {
				"mode" : "trim_single_replica",
				"log_errors" : "true"
			    }

                    }
                ]
            }
        }
    )

    irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-tier_group_metadata-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-tier_group_metadata",
            "plugin_specific_configuration": {
                    "log_errors" : "true"
            }
        }
    )

    irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-data_movement-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-data_movement",
            "plugin_specific_configuration": {
                    "log_errors" : "true"
            }
        }
    )

    irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
        {
            "instance_name": "irods_rule_engine_plugin-policy_engine-data_restage-instance",
            "plugin_name": "irods_rule_engine_plugin-policy_engine-data_restage",
            "plugin_specific_configuration": {
                    "log_errors" : "true"
            }
        }
    )

    if("replication" != without):
        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-policy_engine-data_replication-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-data_replication",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
            }
        )

    if("verification" != without):
        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-policy_engine-data_verification-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-data_verification",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
            }
        )

    if("retention" != without):
        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-policy_engine-data_retention-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-data_retention",
                "plugin_specific_configuration": {
                    "mode" : "trim_single_replica",
                    "log_errors" : "true"
                }
            }
        )

    if("access_time" != without):
        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-policy_engine-access_time-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-access_time",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )

    if("query_processor" != without):
        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-policy_engine-query_processor-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-query_processor",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )

    if("event_generator-resource_metadata" != without):
        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
           {
                "instance_name": "irods_rule_engine_plugin-event_generator-resource_metadata-instance",
                "plugin_name": "irods_rule_engine_plugin-event_generator-resource_metadata",
                "plugin_specific_configuration": {
                    "log_errors" : "true"
                }
           }
        )

@contextlib.contextmanager
def storage_tiering_configured_custom(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        insert_plugins(irods_config)

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name" : "irods_rule_engine_plugin-policy_engine-storage_tiering-instance",
                "plugin_name" : "irods_rule_engine_plugin-policy_engine-storage_tiering",
                "plugin_specific_configuration" : {
                    "access_time_attribute" : "irods::custom_access_time",
                    "group_attribute" : "irods::custom_storage_tiering::group",
                    "time_attribute" : "irods::custom_storage_tiering::time",
                    "query_attribute" : "irods::custom_storage_tiering::query",
                    "verification_attribute" : "irods::custom_storage_tiering::verification",
                    "restage_delay_attribute" : "irods::custom_storage_tiering::restage_delay",

                    "default_restage_delay_parameters" : "<PLUSET>1s</PLUSET>",
                    "time_check_string" : "TIME_CHECK_STRING",
                    "log_errors" : "true"
                }
            }
        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        insert_plugins(irods_config)

#        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
#            {
#                "instance_name": "irods_rule_engine_plugin-policy_engine-storage_tiering-instance",
#                "plugin_name": "irods_rule_engine_plugin-policy_engine-storage_tiering",
#                "plugin_specific_configuration": {
#                    "data_transfer_log_level" : "LOG_NOTICE",
#                    "log_errors" : "true"
#                }
#            }
#        )

        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

@contextlib.contextmanager
def storage_tiering_configured_with_log(arg=None):
    filename = paths.server_config_path()
    with lib.file_backed_up(filename):
        irods_config = IrodsConfig()
        irods_config.server_config['advanced_settings']['rule_engine_server_sleep_time_in_seconds'] = 1

        insert_plugins(irods_config)

        irods_config.server_config['plugin_configuration']['rule_engines'].insert(0,
            {
                "instance_name": "irods_rule_engine_plugin-policy_engine-storage_tiering-instance",
                "plugin_name": "irods_rule_engine_plugin-policy_engine-storage_tiering",
                "plugin_specific_configuration": {
                    "data_transfer_log_level" : "LOG_NOTICE",
                    "log_errors" : "true"
                }
            }
        )


        irods_config.commit(irods_config.server_config, irods_config.server_config_path)
        try:
            yield
        finally:
            pass

def wait_for_empty_queue(function):
# TODO Need a failure mode, all exceptions are ignored
    done = False
    while done == False:
        out, err, rc = lib.execute_command_permissive(['iqstat', '-a'])
        if -1 != out.find('No delayed rules pending'):
            try:
                function()
            except:
                pass
            done = True
        else:
            print('    Output ['+out+']')
            sleep(1)

movement_rule = """
{
            "policy" : "irods_policy_execute_rule",
            "payload" : {
                 "policy_to_invoke" : "irods_policy_query_processor",
                 "configuration" : {
                     "query_string" : "SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = 'irods::storage_tiering::group'",
                     "query_limit" : 0,
                     "query_type" : "general",
                     "number_of_threads" : 8,
                     "policy_to_invoke" : "irods_policy_event_generator_resource_metadata",
                     "configuration" : {
                         "conditional" : {
                             "metadata" : {
                                 "attribute" : "irods::storage_tiering::group",
                                 "value" : "{0}"
                             }
                         },
                         "policies_to_invoke" : [
                             {
                                 "policy" : "irods_policy_query_processor",
                                 "configuration" : {
                                     "query_string" : "SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = 'irods::storage_tiering::query' AND RESC_NAME = 'IRODS_TOKEN_SOURCE_RESOURCE_END_TOKEN'",
                                     "default_results_when_no_rows_found" : ["SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE META_DATA_ATTR_NAME = 'irods::access_time' AND META_DATA_ATTR_VALUE < 'IRODS_TOKEN_LIFETIME_END_TOKEN' AND META_DATA_ATTR_UNITS <> 'irods::storage_tiering::inflight' AND DATA_RESC_ID IN (IRODS_TOKEN_SOURCE_RESOURCE_LEAF_BUNDLE_END_TOKEN)"],
                                     "query_limit" : 0,
                                     "query_type" : "general",
                                     "number_of_threads" : 8,
                                     "policy_to_invoke" : "irods_policy_query_processor",
                                     "configuration" : {
                                         "lifetime" : "IRODS_TOKEN_QUERY_SUBSTITUTION_END_TOKEN(SELECT META_RESC_ATTR_VALUE WHERE META_RESC_ATTR_NAME = 'irods::storage_tiering::time' AND RESC_NAME = 'IRODS_TOKEN_SOURCE_RESOURCE_END_TOKEN')",
                                         "query_string" : "{0}",
                                         "query_limit" : 0,
                                         "query_type" : "general",
                                         "number_of_threads" : 8,
                                         "policy_to_invoke" : "irods_policy_data_replication",
                                         "configuration" : {
                                             "comment" : "source_resource, and destination_resource supplied by the resource metadata event generator"
                                         }
                                     }
                                 }
                             }
                        ]
                     }
                 }
            }
}
INPUT null
OUTPUT ruleExecOut
"""




class TestStorageTieringPlugin(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPlugin, self).setUp()

        with open('example_movement_rule.r', 'w') as f:
            f.write(movement_rule)

        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs3 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs3', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs4 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs4', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs5 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs5', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc rnd0 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd1 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd2 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs5')

            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::time 15')
# TODO consider the inflight use case
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tiering::query "SELECT USER_NAME, COLL_NAME, DATA_NAME, RESC_NAME WHERE META_DATA_ATTR_NAME = 'irods::access_time' AND META_DATA_ATTR_VALUE < 'IRODS_TOKEN_LIFETIME_END_TOKEN' AND META_DATA_ATTR_UNITS <> 'irods::storage_tiering::inflight' AND DATA_RESC_ID IN (IRODS_TOKEN_SOURCE_RESOURCE_LEAF_BUNDLE_END_TOKEN)"''')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPlugin, self).tearDown()

        os.remove('example_movement_rule.r')

        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs5')

            admin_session.assert_icommand('iadmin rmresc rnd0')
            admin_session.assert_icommand('iadmin rmresc rnd1')
            admin_session.assert_icommand('iadmin rmresc rnd2')
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rmresc ufs3')
            admin_session.assert_icommand('iadmin rmresc ufs4')
            admin_session.assert_icommand('iadmin rmresc ufs5')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        rule_file = tempfile.NamedTemporaryFile(mode='wt', dir='/tmp', delete=False).name + '.r'
        with open(rule_file, 'w') as f:
            f.write(movement_rule)
        with storage_tiering_configured():
            IrodsController().restart()
            zone_name = IrodsConfig().client_environment['irods_zone_name']
            with session.make_session_for_existing_admin() as admin_session:
		admin_session.assert_icommand('ilsresc -l', 'STDOUT_SINGLELINE', 'demoResc')
                with session.make_session_for_existing_user('alice', 'apass', lib.get_hostname(), zone_name) as alice_session:
                    filename = 'test_put_file'
                    lib.create_local_testfile(filename)
                    alice_session.assert_icommand('iput -R rnd0 ' + filename)
                    alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                    alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

		    # test stage to tier 1
		    sleep(8)
		    admin_session.assert_icommand('irule -r irods_rule_engine_plugin-cpp_default_policy-instance -F ' + rule_file, 'STDOUT_SINGLELINE', 'deprecated')
		    wait_for_empty_queue(lambda: alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1'))
                    alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)

		    # test stage to tier 2
		    sleep(18)
		    admin_session.assert_icommand('irule -r irods_rule_engine_plugin-cpp_default_policy-instance -F ' + rule_file, 'STDOUT_SINGLELINE', 'deprecated')
		    wait_for_empty_queue(lambda: alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2'))
                    alice_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)

		    # test restage to tier 0
		    alice_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
		    wait_for_empty_queue(lambda: alice_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0'))

		    alice_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginMultiGroup(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginMultiGroup, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs3 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs3', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs4 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs4', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs5 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs5', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc rnd0 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd1 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd2 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs5')

            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, DATA_REPL_NUM  where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')

            admin_session.assert_icommand('iadmin mkresc ufs0g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1g2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2g2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2g2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tiering::group example_group_g2 0')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tiering::group example_group_g2 1')
            admin_session.assert_icommand('imeta add -R ufs2g2 irods::storage_tiering::group example_group_g2 2')

            admin_session.assert_icommand('imeta add -R ufs0g2 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1g2 irods::storage_tiering::time 65')

            admin_session.assert_icommand('''imeta set -R ufs1g2 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, DATA_REPL_NUM where RESC_NAME = 'ufs1g2' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginMultiGroup, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs5')

            admin_session.assert_icommand('iadmin rmresc rnd0')
            admin_session.assert_icommand('iadmin rmresc rnd1')
            admin_session.assert_icommand('iadmin rmresc rnd2')
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rmresc ufs3')
            admin_session.assert_icommand('iadmin rmresc ufs4')
            admin_session.assert_icommand('iadmin rmresc ufs5')

            admin_session.assert_icommand('iadmin rmresc ufs0g2')
            admin_session.assert_icommand('iadmin rmresc ufs1g2')
            admin_session.assert_icommand('iadmin rmresc ufs2g2')

            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                print("yep")
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                filename = 'test_put_file'

                filenameg2 = 'test_put_fileg2'
                lib.create_local_testfile(filenameg2)

                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                admin_session.assert_icommand('iput -R ufs0g2 ' + filenameg2)
                admin_session.assert_icommand('imeta ls -d ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)
                admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', filenameg2)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs1g2'))

                # test stage to tier 2
                sleep(15)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs2g2'))

                # test restage to tier 0
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                admin_session.assert_icommand('iget ' + filenameg2 + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filenameg2, 'STDOUT_SINGLELINE', 'ufs0g2'))


                admin_session.assert_icommand('irm -f ' + filename)
                admin_session.assert_icommand('irm -f ' + filenameg2)

class TestStorageTieringPluginCustomMetadata(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginCustomMetadata, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs3 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs3', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs4 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs4', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs5 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs5', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc rnd0 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd1 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin mkresc rnd2 random', 'STDOUT_SINGLELINE', 'random')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin addchildtoresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin addchildtoresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin addchildtoresc rnd2 ufs5')

            admin_session.assert_icommand('imeta add -R rnd0 irods::custom_storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R rnd2 irods::custom_storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R rnd0 irods::custom_storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R rnd1 irods::custom_storage_tiering::time 15')
            admin_session.assert_icommand('''imeta set -R rnd1 irods::custom_storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, DATA_REPL_NUM where RESC_NAME = 'ufs2' || = 'ufs3' and META_DATA_ATTR_NAME = 'irods::custom_access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R rnd1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginCustomMetadata, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs0')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd0 ufs1')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs2')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd1 ufs3')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs4')
            admin_session.assert_icommand('iadmin rmchildfromresc rnd2 ufs5')

            admin_session.assert_icommand('iadmin rmresc rnd0')
            admin_session.assert_icommand('iadmin rmresc rnd1')
            admin_session.assert_icommand('iadmin rmresc rnd2')
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rmresc ufs3')
            admin_session.assert_icommand('iadmin rmresc ufs4')
            admin_session.assert_icommand('iadmin rmresc ufs5')
            admin_session.assert_icommand('iadmin rum')


    def test_put_and_get(self):
        with storage_tiering_configured_custom():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                admin_session.assert_icommand('iput -R rnd0 ' + filename)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd1'))

                # test stage to tier 2
                sleep(15)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd2'))

                # test restage to tier 0
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'rnd0'))

                admin_session.assert_icommand('irm -f ' + filename)

#   class TestStorageTieringPluginWithMungefs(ResourceBase, unittest.TestCase):
#       def setUp(self):
#           super(TestStorageTieringPluginWithMungefs, self).setUp()
#           with session.make_session_for_existing_admin() as admin_session:
#               # start with a fresh queue
#               admin_session.assert_icommand('iqdel -a')
#
#               # configure mungefs
#               self.munge_mount=tempfile.mkdtemp(prefix='munge_mount_')
#               self.munge_target=tempfile.mkdtemp(prefix='munge_target_')
#
#               munge_cmd = 'mungefs ' + self.munge_mount + ' -omodules=subdir,subdir=' + self.munge_target
#
#               print('MUNGE_CMD ['+munge_cmd+']')
#
#               assert_command(munge_cmd)
#
#               munge_link = '/var/lib/irods/msiExecCmd_bin/mungefsctl'
#               if os.path.exists(munge_link):
#                   os.unlink(munge_link)
#
#               os.symlink('/usr/bin/mungefsctl', munge_link)
#
#               admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
#               admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':'+self.munge_mount, 'STDOUT_SINGLELINE', 'unixfilesystem')
#
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
#               admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
#
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
#
#       def tearDown(self):
#           super(TestStorageTieringPluginWithMungefs, self).tearDown()
#           if os.path.exists('/var/lib/irods/msiExecCmd_bin/mungefsctl'):
#               os.unlink('/var/lib/irods/msiExecCmd_bin/mungefsctl')
#
#           with session.make_session_for_existing_admin() as admin_session:
#               assert_command('fusermount -u '+self.munge_mount)
#               shutil.rmtree(self.munge_mount, ignore_errors=True)
#               shutil.rmtree(self.munge_target, ignore_errors=True)
#
#               admin_session.assert_icommand('iadmin rmresc ufs0')
#               admin_session.assert_icommand('iadmin rmresc ufs1')
#               admin_session.assert_icommand('iadmin rum')
#
#       def test_put_verify_filesystem(self):
#           IrodsController().restart()
#           with storage_tiering_configured_without_replication():
#               with session.make_session_for_existing_admin() as admin_session:
#                   core_re = paths.core_re_directory() + "/core.re"
#                   with lib.file_backed_up(core_re):
#                       sleep(1)  # remove once file hash fix is committed #2279
#                       lib.prepend_string_to_file("""\n
#       # replicate the data then engage MungeFs\n
#       irods_policy_data_replication(*inst_name, *src_resc, *dst_resc, *obj_path) {\n
#           writeLine("serverLog", "irods_policy_data_replication :: [*inst_name] [*src_resc] [*dst_resc] [*obj_path]");\n
#           *err = errormsg(msiDataObjRepl(\n
#                               *obj_path,\n
#                               "rescName=*src_resc++++destRescName=*dst_resc",\n
#                               *out_param), *msg)\n
#           if(0 != *err) {\n
#               failmsg(*err, "msiDataObjRepl failed for [*obj_path] [*src_resc] [*dst_resc] - [*msg]")\n
#           }\n
#           \n
#           # configure mungefs to report an invalid file size\n
#           writeLine("serverLog", "irods_policy_data_replication :: setting mungefs to corrupt_size");\n
#           *err = errormsg(msiExecCmd("mungefsctl", "--operations 'getattr' --corrupt_size", "null", "null", "null", *std_out_err), *msg)\n
#           if(0 != *err) {\n
#               failmsg(*err, "msiDataObjRepl failed for [*obj_path] [*src_resc] [*dst_resc] - [*msg]")\n
#           }\n
#           msiSleep("2", "0")\n
#       }""", core_re)
#                       sleep(1)  # remove once file hash fix is committed #2279
#
#                       # restart the server to reread the new core.re
#                       IrodsController().restart()
#
#                       # set filesystem verification
#                       admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::verification filesystem')
#
#                       # debug
#                       admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')
#
#                       filename = 'test_put_file'
#                       filepath = lib.create_local_testfile(filename)
#                       admin_session.assert_icommand('iput -R ufs0 ' + filename)
#                       admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
#                       admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)
#
#                       # test stage to tier 1
#                       sleep(5)
#                       initial_size_of_server_log = lib.get_file_size_by_path(paths.server_log_path())
#                       admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
#                       sleep(60)
#                       log_cnt = lib.count_occurrences_of_string_in_log(
#                               paths.server_log_path(),
#                               'UNMATCHED_KEY_OR_INDEX',
#                               start_index=initial_size_of_server_log)
#
#                       admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')
#
#                       # clean up
#                       assert_command('mungefsctl --operations "getattr"')
#                       admin_session.assert_icommand('irm -f ' + filename)
#                       admin_session.assert_icommand('imeta rm -R ufs1 irods::storage_tiering::verification filesystem')
#
#                       self.assertTrue(log_cnt > 0, msg='log_cnt:{}'.format(log_cnt))

#
#   def test_put_verify_checksum(self):
#       IrodsController().restart()
#       with storage_tiering_configured_without_replication():
#           with session.make_session_for_existing_admin() as admin_session:
#               core_re = paths.core_re_directory() + "/core.re"
#               with lib.file_backed_up(core_re):
#                   sleep(1)  # remove once file hash fix is committed #2279
#                   lib.prepend_string_to_file("""\n
#   # replicate the data then engage MungeFS\n
#   irods_policy_data_replication(*inst_name, *src_resc, *dst_resc, *obj_path) {\n
#       writeLine("serverLog", "irods_policy_data_replication :: [*inst_name] [*src_resc] [*dst_resc] [*obj_path]");\n
#       *err = errormsg(msiDataObjRepl(\n
#                           *obj_path,\n
#                           "rescName=*src_resc++++destRescName=*dst_resc",\n
#                           *out_param), *msg)\n
#       if(0 != *err) {\n
#           failmsg(*err, "msiDataObjRepl failed for [*obj_path] [*src_resc] [*dst_resc] - [*msg]")\n
#       }\n
#       \n
#       # configure mungefs to report an invalid file size\n
#       writeLine("serverLog", "irods_policy_data_replication :: setting mungefs to 'read' corrupt_data");\n
#       *err = errormsg(msiExecCmd("mungefsctl", "--operations 'read' --corrupt_data", "null", "null", "null", *std_out_err), *msg)\n
#       if(0 != *err) {\n
#           failmsg(*err, "msiDataObjRepl failed for [*obj_path] [*src_resc] [*dst_resc] - [*msg]")\n
#       }\n
#       msiSleep("2", "0")\n
#   }""", core_re)
#                   sleep(1)  # remove once file hash fix is committed #2279
#
#                   # restart the server to reread the new core.re
#                   IrodsController().restart()
#
#                   # configure mungefs to report an invalid file size
#                   #assert_command('mungefsctl --operations "read" --corrupt_data')
#
#                   # set checksum verification
#                   admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::verification checksum')
#
#                   # debug
#                   admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')
#
#                   filename = 'test_put_file'
#                   filepath = lib.create_local_testfile(filename)
#                   admin_session.assert_icommand('iput -R ufs0 ' + filename)
#                   admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
#                   admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)
#
#                   # test stage to tier 1
#                   sleep(5)
#                   initial_size_of_server_log = lib.get_file_size_by_path(paths.server_log_path())
#                   admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
#                   sleep(60)
#                   admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')
#
#                   log_cnt = lib.count_occurrences_of_string_in_log(
#                           paths.server_log_path(),
#                           'UNMATCHED_KEY_OR_INDEX',
#                           start_index=initial_size_of_server_log)
#
#                   admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0')
#
#                   # clean up
#                   assert_command('mungefsctl --operations "read"')
#                   admin_session.assert_icommand('irm -f ' + filename)
#                   admin_session.assert_icommand('imeta rm -R ufs1 irods::storage_tiering::verification checksum')
#
#                   self.assertTrue(log_cnt > 0, msg='log_cnt:{}'.format(log_cnt))

class TestStorageTieringPluginMinimumRestage(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginMinimumRestage, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginMinimumRestage, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))

                sleep(15)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs2'))

                # test restage to tier 1
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginPreserveReplica(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginPreserveReplica, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::preserve_replicas true')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

    def tearDown(self):
        super(TestStorageTieringPluginPreserveReplica, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
         with storage_tiering_configured():
             with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # stage to tier 1, look for both replicas
                sleep(10)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))

                # test prevent retier from preserved replica
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs0'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs2'))

                admin_session.assert_icommand('irm -f ' + filename)

class TestStorageTieringPluginObjectLimit(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginObjectLimit, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 2')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 15')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_restage_tier true')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')

            self.filename  = 'test_put_file'
            self.filename2 = 'test_put_file2'

    def tearDown(self):
        super(TestStorageTieringPluginObjectLimit, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get_limit_1(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::object_limit 1')

                filepath  = lib.create_local_testfile(self.filename)
                admin_session.assert_icommand('iput -R ufs0 ' + self.filename)
                admin_session.assert_icommand('iput -R ufs0 ' + self.filename + " " + self.filename2)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # stage to tier 1, look for both replicas (only one should move)
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs0'))

                admin_session.assert_icommand('irm -f ' + self.filename)
                admin_session.assert_icommand('irm -f ' + self.filename2)

    def test_put_and_get_no_limit_zero(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::object_limit 0')

                filepath  = lib.create_local_testfile(self.filename)
                admin_session.assert_icommand('iput -R ufs0 ' + self.filename)
                admin_session.assert_icommand('iput -R ufs0 ' + self.filename + " " + self.filename2)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # stage to tier 1, everything should move
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -f ' + self.filename)
                admin_session.assert_icommand('irm -f ' + self.filename2)

    def test_put_and_get_no_limit_default(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                filepath  = lib.create_local_testfile(self.filename)
                admin_session.assert_icommand('iput -R ufs0 ' + self.filename)
                admin_session.assert_icommand('iput -R ufs0 ' + self.filename + " " + self.filename2)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # stage to tier 1, everything should move
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -f ' + self.filename)
                admin_session.assert_icommand('irm -f ' + self.filename2)

#   class TestStorageTieringPluginLogMigration(ResourceBase, unittest.TestCase):
#       def setUp(self):
#           super(TestStorageTieringPluginLogMigration, self).setUp()
#           with session.make_session_for_existing_admin() as admin_session:
#               admin_session.assert_icommand('iqdel -a')
#               admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
#               admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
#
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
#               admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')
#
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
#               admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
#
#               self.max_sql_rows = 256
#
#       def tearDown(self):
#           super(TestStorageTieringPluginLogMigration, self).tearDown()
#           with session.make_session_for_existing_admin() as admin_session:
#               admin_session.assert_icommand('iadmin rmresc ufs0')
#               admin_session.assert_icommand('iadmin rmresc ufs1')
#               admin_session.assert_icommand('iadmin rum')
#
#       def test_put_and_get(self):
#           with storage_tiering_configured_with_log():
#               with session.make_session_for_existing_admin() as admin_session:
#
#                       initial_log_size = lib.get_file_size_by_path(paths.server_log_path())
#
#                       filename = 'test_put_file'
#                       filepath = lib.create_local_testfile(filename)
#                       admin_session.assert_icommand('iput -R ufs0 ' + filename)
#                       admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
#                       admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)
#
#                       # test stage to tier 1
#                       sleep(5)
#                       admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
#                       sleep(60)
#
#                       admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1')
#                       admin_session.assert_icommand('irm -f ' + filename)
#
#                       log_count = lib.count_occurrences_of_string_in_log(paths.server_log_path(), 'irods::storage_tiering migrating', start_index=initial_log_size)
#                       self.assertTrue(1 == log_count, msg='log_count:{}'.format(log_count))

class TestStorageTieringMultipleQueries(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringMultipleQueries, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('''iadmin asq "select distinct R_DATA_MAIN.data_name, R_COLL_MAIN.coll_name, R_DATA_MAIN.data_owner_name, R_DATA_MAIN.data_repl_num from R_DATA_MAIN, R_COLL_MAIN, R_RESC_MAIN, R_OBJT_METAMAP r_data_metamap, R_META_MAIN r_data_meta_main where R_RESC_MAIN.resc_name = 'ufs0' AND r_data_meta_main.meta_attr_name = 'archive_object' AND r_data_meta_main.meta_attr_value = 'yes' AND R_COLL_MAIN.coll_id = R_DATA_MAIN.coll_id AND R_RESC_MAIN.resc_id = R_DATA_MAIN.resc_id AND R_DATA_MAIN.data_id = r_data_metamap.object_id AND r_data_metamap.meta_id = r_data_meta_main.meta_id order by R_COLL_MAIN.coll_name, R_DATA_MAIN.data_name" archive_query''')

            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 15')

            admin_session.assert_icommand('''imeta add -R ufs0 irods::storage_tiering::query "SELECT DATA_NAME, COLL_NAME, USER_NAME, DATA_REPL_NUM where RESC_NAME = 'ufs0' and META_DATA_ATTR_NAME = 'irods::access_time' and META_DATA_ATTR_VALUE < 'TIME_CHECK_STRING'"''')
            admin_session.assert_icommand('''imeta add -R ufs0 irods::storage_tiering::query archive_query specific''')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')


    def tearDown(self):
        super(TestStorageTieringMultipleQueries, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')
            admin_session.assert_icommand('iadmin rsq archive_query')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:

                filename  = 'test_put_file'
                filename2 = 'test_put_file2'
                filepath  = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs0 ' + filename)
                admin_session.assert_icommand('imeta add -d ' + filename + ' archive_object yes')
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', 'irods::access_time')

                admin_session.assert_icommand('iput -R ufs0 ' + filename + ' ' + filename2)
                admin_session.assert_icommand('imeta ls -d ' + filename2, 'STDOUT_SINGLELINE', 'irods::access_time')

                # test stage to tier 1
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs0'))

                sleep(15)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename2, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -f ' + filename)
                admin_session.assert_icommand('irm -f ' + filename2)

class TestStorageTieringPluginRegistration(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginRegistration, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')


    def tearDown(self):
        super(TestStorageTieringPluginRegistration, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    def test_file_registration(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:

                filename  = 'test_put_file'
                filepath  = lib.create_local_testfile(filename)
                ipwd, _, _ = admin_session.run_icommand('ipwd')
                ipwd = ipwd.rstrip()
                dest_path = ipwd + '/' + filename

                admin_session.assert_icommand('ipwd', 'STDOUT_SINGLELINE', 'rods')
                admin_session.assert_icommand('ireg -R ufs0 ' + filepath + ' ' + dest_path)
                admin_session.assert_icommand('imeta ls -d ' + filename, 'STDOUT_SINGLELINE', filename)
                admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', filename)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -f ' + filename)

    def test_directory_registration(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                local_dir_name = '/tmp/test_directory_registration_dir'
                shutil.rmtree(local_dir_name, ignore_errors=True)
                local_tree = lib.make_deep_local_tmp_dir(local_dir_name, 3, 10, 5)

                dest_path = '/tempZone/home/rods/reg_coll'
                admin_session.assert_icommand('ireg -CR ufs0 ' + local_dir_name + ' ' + dest_path)
                admin_session.assert_icommand('ils -rL ' + dest_path, 'STDOUT_SINGLELINE', dest_path)

                # test stage to tier 1
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand('ils -L ' + dest_path, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -rf ' + dest_path)



class TestStorageTieringContinueInxMigration(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringContinueInxMigration, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')

            self.max_sql_rows = 256

    def tearDown(self):
        super(TestStorageTieringContinueInxMigration, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rum')

    def test_put_gt_max_sql_rows(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                # Put enough objects to force continueInx when iterating over violating objects (above MAX_SQL_ROWS)
                file_count = self.max_sql_rows + 1
                dirname = 'test_put_gt_max_sql_rows'
                shutil.rmtree(dirname, ignore_errors=True)
                lib.make_large_local_tmp_dir(dirname, file_count, 1)
                admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname], 'STDOUT_SINGLELINE', ustrings.recurse_ok_string())

                # stage to tier 1, everything should have been tiered out
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand_fail(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0'))

                # cleanup
                admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                shutil.rmtree(dirname, ignore_errors=True)

    def test_put_max_sql_rows(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                # Put exactly MAX_SQL_ROWS objects (boundary test)
                file_count = self.max_sql_rows
                dirname = 'test_put_max_sql_rows'
                shutil.rmtree(dirname, ignore_errors=True)
                lib.make_large_local_tmp_dir(dirname, file_count, 1)
                admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname], 'STDOUT_SINGLELINE', ustrings.recurse_ok_string())

                # stage to tier 1, everything should have been tiered out
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand_fail(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0'))

                # cleanup
                admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                shutil.rmtree(dirname, ignore_errors=True)

    def test_put_object_limit_lt(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                # Put enough objects to force continueInx and set object_limit to one less than that (above MAX_SQL_ROWS)
                file_count = self.max_sql_rows + 2
                admin_session.assert_icommand(['imeta', 'add', '-R', 'ufs0', 'irods::storage_tiering::object_limit', str(file_count - 1)])
                dirname = 'test_put_object_limit_lt'
                shutil.rmtree(dirname, ignore_errors=True)
                last_item_path = os.path.join(dirname, 'junk0' + str(file_count - 1))
                next_to_last_item_path = os.path.join(dirname, 'junk0' + str(file_count - 2))
                lib.make_large_local_tmp_dir(dirname, file_count, 1)
                admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname], 'STDOUT_SINGLELINE', ustrings.recurse_ok_string())

                # stage to tier 1, only the last item should not have been tiered out
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                admin_session.assert_icommand('iqstat', 'STDOUT_SINGLELINE', 'irods_policy_storage_tiering')
                wait_for_empty_queue(lambda: admin_session.assert_icommand(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand(['ils', '-l', last_item_path], 'STDOUT_SINGLELINE', 'ufs0'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand_fail(['ils', '-l', next_to_last_item_path], 'STDOUT_SINGLELINE', 'ufs0'))

                # cleanup
                admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                shutil.rmtree(dirname, ignore_errors=True)

    def test_put_multi_fetch_page(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                # Put enough objects to force results paging more than once
                file_count = (self.max_sql_rows * 2) + 1
                dirname = 'test_put_gt_max_sql_rows'
                shutil.rmtree(dirname, ignore_errors=True)
                lib.make_large_local_tmp_dir(dirname, file_count, 1)
                admin_session.assert_icommand(['iput', '-R', 'ufs0', '-r', dirname], 'STDOUT_SINGLELINE', ustrings.recurse_ok_string())

                # stage to tier 1, everything should have been tiered out
                sleep(5)
                admin_session.assert_icommand('irule -r irods_rule_engine_plugin-storage_tiering-instance -F /var/lib/irods/example_tiering_invocation.r')
                wait_for_empty_queue(lambda: admin_session.assert_icommand(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs1'))
                wait_for_empty_queue(lambda: admin_session.assert_icommand_fail(['ils', '-l', dirname], 'STDOUT_SINGLELINE', 'ufs0'))

                # cleanup
                admin_session.assert_icommand(['irm', '-f', '-r', dirname])
                shutil.rmtree(dirname, ignore_errors=True)

class TestStorageTieringPluginMultiGroupRestage(ResourceBase, unittest.TestCase):
    def setUp(self):
        super(TestStorageTieringPluginMultiGroupRestage, self).setUp()
        with session.make_session_for_existing_admin() as admin_session:
            admin_session.assert_icommand('iqdel -a')
            admin_session.assert_icommand('iadmin mkresc ufs0 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs0', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs1 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs1', 'STDOUT_SINGLELINE', 'unixfilesystem')
            admin_session.assert_icommand('iadmin mkresc ufs2 unixfilesystem '+test.settings.HOSTNAME_1 +':/tmp/irods/ufs2', 'STDOUT_SINGLELINE', 'unixfilesystem')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::group example_group 0')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group 1')

            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::group example_group2 0')
            admin_session.assert_icommand('imeta add -R ufs2 irods::storage_tiering::group example_group2 1')

            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::time 5')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs0 irods::storage_tiering::maximum_delay_time_in_seconds 2')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::minimum_delay_time_in_seconds 1')
            admin_session.assert_icommand('imeta add -R ufs1 irods::storage_tiering::maximum_delay_time_in_seconds 2')


    def tearDown(self):
        super(TestStorageTieringPluginMultiGroupRestage, self).tearDown()
        with session.make_session_for_existing_admin() as admin_session:

            admin_session.assert_icommand('iadmin rmresc ufs0')
            admin_session.assert_icommand('iadmin rmresc ufs1')
            admin_session.assert_icommand('iadmin rmresc ufs2')
            admin_session.assert_icommand('iadmin rum')

    def test_put_and_get(self):
        with storage_tiering_configured():
            IrodsController().restart()
            with session.make_session_for_existing_admin() as admin_session:
                filename = 'test_put_file'
                filepath = lib.create_local_testfile(filename)
                admin_session.assert_icommand('iput -R ufs1 ' + filename)
                admin_session.assert_icommand('ils -L ', 'STDOUT_SINGLELINE', 'rods')

                # test restage to tier 1
                admin_session.assert_icommand('iget ' + filename + ' - ', 'STDOUT_SINGLELINE', 'TESTFILE')
                wait_for_empty_queue(admin_session.assert_icommand('ils -L ' + filename, 'STDOUT_SINGLELINE', 'ufs1'))

                admin_session.assert_icommand('irm -f ' + filename)




