#!/bin/bash
SELFTEST=1 ~/ss/target/release/sessions 2>&1 | tail -1
