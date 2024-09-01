#!/bin/zsh

# uncomment for debugging
# set -x

function syncFiles() {
  local BUCKET_NAME=$1
  aws s3 sync s3://"$BUCKET_NAME" .
}

function addHeader() {
    local OUTPUT_FILE=$1
    echo "# Data" > "$OUTPUT_FILE"
    echo "> Data source: <https://opentransportdata.swiss/de/dataset/istdaten/>" >> "$OUTPUT_FILE"
}

function addActualData() {
    local OUTPUT_FILE=$1
    echo "## Actual Data" >> "$OUTPUT_FILE"
    for file in ./actual-data/**/*.csv
    do
      basename=$(basename "$file")
      echo "- [$basename]($file)" >> "$OUTPUT_FILE"
    done
}

function addDailyCharts() {
    local OUTPUT_FILE=$1
    echo "## Daily Charts" >> "$OUTPUT_FILE"

    current_month=-1
    for file in ./daily-charts/**/*.png""
    do
      basename=$(basename "$file")
      parts=("${(@s:_:)basename}")
      month=${parts[1]}

      if [ "$current_month" != "$month" ]; then
            echo "### $month" >> "$OUTPUT_FILE"
            current_month=$month
      fi
      echo "![chart]($file)" >> "$OUTPUT_FILE"
    done
}

function addMonthlyCharts() {
    local OUTPUT_FILE=$1
    echo "## Monthly Charts" >> "$OUTPUT_FILE"

    current_month=-1
    current_route=-1
    for file in ./monthly-charts/**/*.png
    do
      basename=$(basename "$file")
      parts=("${(@s:_:)basename}")
      month=${parts[1]}
      route=${(C)parts[2]/-/ }

      if [ "$current_route" != "$route" ]; then
          echo "### $route" >> "$OUTPUT_FILE"
          current_route=$route
      fi
      if [ "$current_month" != "$month" ]; then
          echo "#### $month" >> "$OUTPUT_FILE"
          current_month=$month
      fi
      echo "![chart]($file)" >> "$OUTPUT_FILE"
    done
}

function usage() {
  echo "Usage: $0 --bucket-name=<bucket name>"
  echo "  --bucket-name=<bucket name>       Specify the s3 bucket name to sync data from."
}

function main() {
  local output_file="results.md"
  local bucket_name

  for i in "$@"; do
    case $i in
      --bucket-name=*)
        bucket_name="${i#*=}"
        shift
        ;;
      *)
        error "Unknown option: ${i}"
        usage
        exit 1
        ;;
    esac
  done
  if [ -z "$bucket_name" ]; then
      error 'Command line argument --bucket-name is missing'
      usage
      exit 1
  fi

  syncFiles "stibusdelaysstack-databucketa7e4f76c-indq3e5gqxxy"
  addHeader "$output_file"
  addMonthlyCharts "$output_file"
  addDailyCharts "$output_file"
  addActualData "$output_file"
}

main "$@"