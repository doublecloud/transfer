## Known differences between gp & pg schemas

greenplum specific:

* null as domain_name,
* data_type_verbose::text as data_type_underlying_under_domain,
* null as all_enum_values,

## file stub.s

file stub.s is needed to hide IDE warning 'missing function body' - see https://github.com/golang/go/issues/15006
