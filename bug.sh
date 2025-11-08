#!/bin/bash

redis-cli SET one aaa && redis-cli GET one
redis-cli SET two bbb && redis-cli GET two
redis-cli GET one
redis-cli GET two
redis-cli GET two
redis-cli SET one aaa && redis-cli GET one
