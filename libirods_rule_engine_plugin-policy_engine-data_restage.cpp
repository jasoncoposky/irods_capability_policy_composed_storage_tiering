
#include "policy_engine.hpp"
#include "policy_engine_parameter_capture.hpp"
#include "storage_tiering.hpp"

namespace pe = irods::policy_engine;

namespace {

    irods::error data_restage_policy(const pe::context& ctx)
    {
        auto [user_name, logical_path, source_resource, tmp_dst_resc] =
                extract_dataobj_inp_parameters(ctx.parameters, tag_first_resc);

        irods::storage_tiering st{ctx.rei, ctx.instance_name};
        st.migrate_object_to_minimum_restage_tier(
              logical_path
            , user_name
            , source_resource);

        return SUCCESS();

    } // data_restage_policy

} // namespace

const char usage[] = R"(
{
    "id": "file:///var/lib/irods/configuration_schemas/v3/policy_engine_usage.json",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": ""
        "input_interfaces": [
            {
                "name" : "event_handler-collection_modified",
                "description" : "",
                "json_schema" : ""
            },
            {
                "name" :  "event_handler-data_object_modified",
                "description" : "",
                "json_schema" : ""
            },
            {
                "name" :  "event_handler-metadata_modified",
                "description" : "",
                "json_schema" : ""
            },
            {
                "name" :  "event_handler-user_modified",
                "description" : "",
                "json_schema" : ""
            },
            {
                "name" :  "event_handler-resource_modified",
                "description" : "",
                "json_schema" : ""
            },
            {
                "name" :  "direct_invocation",
                "description" : "",
                "json_schema" : ""
            },
            {
                "name" :  "query_results"
                "description" : "",
                "json_schema" : ""
            },
        ],
    "output_json_for_validation" : ""
}
)";

extern "C"
pe::plugin_pointer_type plugin_factory(
      const std::string& _plugin_name
    , const std::string&) {

    return pe::make(
                 _plugin_name
               , "irods_policy_data_restage"
               , usage
               , data_restage_policy);

} // plugin_factory
