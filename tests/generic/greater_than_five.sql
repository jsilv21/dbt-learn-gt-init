-- changing a singular test to a generic test

{% test greater_than_five(model, column_name)%}
select 
{{column_name}}
from {{model}}
where {{column_name}} <= 5

{%endtest%}