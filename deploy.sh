#!/bin/bash
#
# Ymir Project Deploy Script
# Replicates the behavior of `ymir project:deploy`
#
# Usage: ./deploy.sh [environment]
# Default environment: staging
#

set -e

# Configuration
ENVIRONMENT="${1:-staging}"
PROJECT_CONFIG=".ymir/project.yml"
CLI_CONFIG="$HOME/.ymir/config.json"
BUILD_DIR="$HOME/.ymir"
BUILD_ZIP="$BUILD_DIR/build.zip"
ASSETS_DIR=".ymir/assets"
API_BASE_URL="https://api.ymirapp.com"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Retry settings
MAX_RETRIES=5
INITIAL_DELAY=1
MAX_DELAY=16

#######################################
# Utility Functions
#######################################

log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1" >&2
}

die() {
    log_error "$1"
    exit 1
}

#######################################
# Step 1: Authentication
#######################################

get_access_token() {
    # Check environment variable first
    if [[ -n "$YMIR_API_TOKEN" ]]; then
        echo "$YMIR_API_TOKEN"
        return
    fi

    # Fall back to config file
    if [[ -f "$CLI_CONFIG" ]]; then
        token=$(jq -r '.token // empty' "$CLI_CONFIG" 2>/dev/null)
        if [[ -n "$token" ]]; then
            echo "$token"
            return
        fi
    fi

    die "No API token found. Set YMIR_API_TOKEN or run 'ymir login'"
}

ACCESS_TOKEN=$(get_access_token)

#######################################
# Step 2: Parse Project Configuration
#######################################

parse_project_config() {
    if [[ ! -f "$PROJECT_CONFIG" ]]; then
        die "Project configuration file not found: $PROJECT_CONFIG"
    fi

    # Check for required tools
    if ! command -v yq &> /dev/null; then
        die "yq is required to parse YAML. Install with: brew install yq (or apt install yq)"
    fi

    PROJECT_ID=$(yq '.id' "$PROJECT_CONFIG")
    PROJECT_NAME=$(yq '.name' "$PROJECT_CONFIG")
    PROJECT_TYPE=$(yq '.type' "$PROJECT_CONFIG")

    if [[ -z "$PROJECT_ID" || "$PROJECT_ID" == "null" ]]; then
        die "Project ID not found in $PROJECT_CONFIG"
    fi

    # Validate environment exists
    ENV_EXISTS=$(yq ".environments.$ENVIRONMENT" "$PROJECT_CONFIG")
    if [[ "$ENV_EXISTS" == "null" ]]; then
        die "Environment '$ENVIRONMENT' not found in project configuration"
    fi

    # Get deployment type
    DEPLOYMENT_TYPE=$(yq ".environments.$ENVIRONMENT.deployment // \"zip\"" "$PROJECT_CONFIG")
    if [[ "$DEPLOYMENT_TYPE" == "null" ]]; then
        DEPLOYMENT_TYPE="zip"
    fi
    # Handle object format: deployment.type
    if [[ "$DEPLOYMENT_TYPE" != "zip" && "$DEPLOYMENT_TYPE" != "image" ]]; then
        DEPLOYMENT_TYPE=$(yq ".environments.$ENVIRONMENT.deployment.type // \"zip\"" "$PROJECT_CONFIG")
    fi

    log_info "Project ID: $PROJECT_ID"
    log_info "Project Name: $PROJECT_NAME"
    log_info "Project Type: $PROJECT_TYPE"
    log_info "Environment: $ENVIRONMENT"
    log_info "Deployment Type: $DEPLOYMENT_TYPE"
}

#######################################
# Step 3: Generate Assets Hash
#######################################

generate_assets_hash() {
    if [[ ! -d "$ASSETS_DIR" ]]; then
        log_warning "Assets directory not found: $ASSETS_DIR"
        ASSETS_HASH=""
        return
    fi

    log_info "Generating assets hash..."

    # Generate hash for each file and combine
    local combined=""
    while IFS= read -r -d '' file; do
        relative_path="${file#.ymir/}"
        file_hash=$(sha256sum "$file" | cut -d' ' -f1)
        combined+="${relative_path}|${file_hash}\n"
    done < <(find "$ASSETS_DIR" -type f -print0 | sort -z)

    if [[ -n "$combined" ]]; then
        ASSETS_HASH=$(echo -e "$combined" | sort | sha256sum | cut -d' ' -f1)
    else
        ASSETS_HASH=""
    fi

    log_info "Assets hash: ${ASSETS_HASH:-none}"
}

#######################################
# Step 4: Build Project (Create ZIP)
#######################################

build_project() {
    log_info "Building project..."

    # Ensure build directory exists
    mkdir -p "$BUILD_DIR"

    # Remove old build zip if exists
    rm -f "$BUILD_ZIP"

    # Get files to include in build based on project type
    # This is a simplified version - actual implementation varies by project type
    local include_patterns=()

    case "$PROJECT_TYPE" in
        bedrock|wordpress)
            include_patterns=(
                "web/app/**"
                "web/wp-config.php"
                "config/**"
                "vendor/**"
                "composer.json"
                "composer.lock"
            )
            ;;
        laravel)
            include_patterns=(
                "app/**"
                "bootstrap/**"
                "config/**"
                "database/**"
                "public/**"
                "resources/**"
                "routes/**"
                "storage/**"
                "vendor/**"
                "artisan"
                "composer.json"
                "composer.lock"
            )
            ;;
        *)
            # Generic: include everything except common excludes
            include_patterns=("**")
            ;;
    esac

    # Get additional includes from config
    ADDITIONAL_INCLUDES=$(yq -r ".environments.$ENVIRONMENT.build.include[]? // empty" "$PROJECT_CONFIG" 2>/dev/null)

    log_info "Creating build.zip..."

    # Create zip file
    # Exclude common files that shouldn't be deployed
    zip -r "$BUILD_ZIP" . \
        -x "*.git*" \
        -x "node_modules/*" \
        -x ".ymir/*" \
        -x "*.env*" \
        -x "tests/*" \
        -x "*.md" \
        -x "deploy.sh" \
        -x ".DS_Store" \
        -x "*.log" \
        > /dev/null 2>&1 || die "Failed to create build.zip"

    # Check file size (max ~147MB uncompressed)
    local zip_size=$(stat -f%z "$BUILD_ZIP" 2>/dev/null || stat -c%s "$BUILD_ZIP" 2>/dev/null)
    log_info "Build zip size: $(numfmt --to=iec-i --suffix=B $zip_size 2>/dev/null || echo "${zip_size} bytes")"

    log_success "Build completed: $BUILD_ZIP"
}

#######################################
# Step 5: API Helper Functions
#######################################

api_request() {
    local method="$1"
    local endpoint="$2"
    local data="$3"
    local retry_count=0
    local delay=$INITIAL_DELAY

    while [[ $retry_count -lt $MAX_RETRIES ]]; do
        local response
        local http_code

        if [[ -n "$data" ]]; then
            response=$(curl -s -w "\n%{http_code}" \
                -X "$method" \
                -H "Authorization: Bearer $ACCESS_TOKEN" \
                -H "Accept: application/json" \
                -H "Content-Type: application/json" \
                -d "$data" \
                "$API_BASE_URL$endpoint" 2>/dev/null)
        else
            response=$(curl -s -w "\n%{http_code}" \
                -X "$method" \
                -H "Authorization: Bearer $ACCESS_TOKEN" \
                -H "Accept: application/json" \
                "$API_BASE_URL$endpoint" 2>/dev/null)
        fi

        http_code=$(echo "$response" | tail -n1)
        body=$(echo "$response" | sed '$d')

        if [[ "$http_code" =~ ^2 ]]; then
            echo "$body"
            return 0
        elif [[ "$http_code" == "401" ]]; then
            die "Authentication failed. Please check your API token."
        elif [[ "$http_code" == "404" ]]; then
            die "Resource not found: $endpoint"
        elif [[ "$http_code" =~ ^5 ]]; then
            # Server error - retry
            ((retry_count++))
            if [[ $retry_count -lt $MAX_RETRIES ]]; then
                log_warning "Server error ($http_code). Retrying in ${delay}s... (attempt $((retry_count+1))/$MAX_RETRIES)"
                sleep $delay
                delay=$((delay * 2))
                [[ $delay -gt $MAX_DELAY ]] && delay=$MAX_DELAY
            fi
        else
            die "API request failed with status $http_code: $body"
        fi
    done

    die "Max retries exceeded for API request: $endpoint"
}

#######################################
# Step 6: Validate Project Configuration
#######################################

validate_project() {
    log_info "Validating project configuration..."

    local config_json
    config_json=$(yq -o=json "$PROJECT_CONFIG")

    local response
    response=$(api_request "POST" "/projects/$PROJECT_ID/validate-configuration" "$config_json")

    # Check for warnings
    local warnings
    warnings=$(echo "$response" | jq -r '.warnings[]? // empty' 2>/dev/null)
    if [[ -n "$warnings" ]]; then
        echo "$warnings" | while read -r warning; do
            log_warning "$warning"
        done
    fi

    log_success "Project configuration validated"
}

#######################################
# Step 7: Create Deployment
#######################################

create_deployment() {
    log_info "Creating deployment..."

    local config_json
    config_json=$(yq -o=json "$PROJECT_CONFIG")

    local request_body
    if [[ -n "$ASSETS_HASH" ]]; then
        request_body=$(jq -n \
            --arg env "$ENVIRONMENT" \
            --arg hash "$ASSETS_HASH" \
            --argjson config "$config_json" \
            '{environment: $env, configuration: $config, assets_hash: $hash}')
    else
        request_body=$(jq -n \
            --arg env "$ENVIRONMENT" \
            --argjson config "$config_json" \
            '{environment: $env, configuration: $config}')
    fi

    local response
    response=$(api_request "POST" "/projects/$PROJECT_ID/deployments" "$request_body")

    DEPLOYMENT_ID=$(echo "$response" | jq -r '.id')
    if [[ -z "$DEPLOYMENT_ID" || "$DEPLOYMENT_ID" == "null" ]]; then
        die "Failed to create deployment: No deployment ID returned"
    fi

    log_success "Deployment created with ID: $DEPLOYMENT_ID"
}

#######################################
# Step 8: Upload Function Code
#######################################

upload_function_code() {
    if [[ "$DEPLOYMENT_TYPE" == "image" ]]; then
        upload_docker_image
    else
        upload_build_zip
    fi
}

upload_build_zip() {
    log_info "Getting artifact upload URL..."

    local response
    response=$(api_request "GET" "/deployments/$DEPLOYMENT_ID/artifact-upload-url")

    local upload_url
    upload_url=$(echo "$response" | jq -r '.url // .upload_url // .' | tr -d '"')

    if [[ -z "$upload_url" || "$upload_url" == "null" ]]; then
        die "Failed to get artifact upload URL"
    fi

    log_info "Uploading build.zip..."

    local retry_count=0
    local delay=$INITIAL_DELAY

    while [[ $retry_count -lt $MAX_RETRIES ]]; do
        local http_code
        http_code=$(curl -s -w "%{http_code}" -o /dev/null \
            -X PUT \
            -H "Cache-Control: public, max-age=2628000" \
            -H "Content-Type: application/zip" \
            --data-binary "@$BUILD_ZIP" \
            "$upload_url" 2>/dev/null)

        if [[ "$http_code" =~ ^2 ]]; then
            log_success "Build artifact uploaded successfully"
            return 0
        else
            ((retry_count++))
            if [[ $retry_count -lt $MAX_RETRIES ]]; then
                log_warning "Upload failed ($http_code). Retrying in ${delay}s... (attempt $((retry_count+1))/$MAX_RETRIES)"
                sleep $delay
                delay=$((delay * 2))
                [[ $delay -gt $MAX_DELAY ]] && delay=$MAX_DELAY
            fi
        fi
    done

    die "Failed to upload build artifact after $MAX_RETRIES attempts"
}

upload_docker_image() {
    log_info "Getting deployment image data..."

    local response
    response=$(api_request "GET" "/deployments/$DEPLOYMENT_ID/image")

    local registry
    local repository
    local tag
    local username
    local password

    registry=$(echo "$response" | jq -r '.registry')
    repository=$(echo "$response" | jq -r '.repository')
    tag=$(echo "$response" | jq -r '.tag')
    username=$(echo "$response" | jq -r '.username')
    password=$(echo "$response" | jq -r '.password')

    log_info "Logging into Docker registry..."
    echo "$password" | docker login "$registry" -u "$username" --password-stdin

    local image_name="${PROJECT_NAME}:${ENVIRONMENT}"
    local remote_image="${registry}/${repository}:${tag}"

    log_info "Tagging image as $remote_image..."
    docker tag "$image_name" "$remote_image"

    log_info "Pushing image to registry..."
    docker push "$remote_image"

    log_success "Docker image uploaded successfully"
}

#######################################
# Step 9: Process Assets
#######################################

process_assets() {
    if [[ ! -d "$ASSETS_DIR" ]] || [[ -z "$(ls -A "$ASSETS_DIR" 2>/dev/null)" ]]; then
        log_info "No assets to process"
        return
    fi

    log_info "Processing assets..."

    # Collect asset information
    local assets_json="["
    local first=true

    while IFS= read -r -d '' file; do
        relative_path="${file#.ymir/assets/}"
        file_hash=$(md5sum "$file" | cut -d' ' -f1)

        if [[ "$first" == true ]]; then
            first=false
        else
            assets_json+=","
        fi

        assets_json+="{\"path\":\"$relative_path\",\"hash\":\"$file_hash\"}"
    done < <(find "$ASSETS_DIR" -type f -print0)

    assets_json+="]"

    # Get signed URLs for assets
    local response
    response=$(api_request "POST" "/deployments/$DEPLOYMENT_ID/signed-asset-requests" "$assets_json")

    # Process copy requests (existing assets)
    local copy_requests
    copy_requests=$(echo "$response" | jq -c '.[] | select(.command == "copy")')

    # Process store requests (new assets)
    local store_requests
    store_requests=$(echo "$response" | jq -c '.[] | select(.command == "store")')

    local copy_count=$(echo "$response" | jq '[.[] | select(.command == "copy")] | length')
    local store_count=$(echo "$response" | jq '[.[] | select(.command == "store")] | length')

    if [[ "$copy_count" -gt 0 ]]; then
        log_info "Copying $copy_count unchanged assets..."
        echo "$copy_requests" | while read -r request; do
            local uri=$(echo "$request" | jq -r '.uri')
            local headers=$(echo "$request" | jq -r '.headers | to_entries | map("-H \"" + .key + ": " + .value + "\"") | join(" ")')

            eval "curl -s -X PUT $headers \"$uri\"" > /dev/null
        done
    fi

    if [[ "$store_count" -gt 0 ]]; then
        log_info "Uploading $store_count new assets..."
        echo "$store_requests" | while read -r request; do
            local uri=$(echo "$request" | jq -r '.uri')
            local relative_path=$(echo "$request" | jq -r '.relative_path')
            local file_path="$ASSETS_DIR/$relative_path"

            if [[ -f "$file_path" ]]; then
                curl -s -X PUT \
                    -H "Cache-Control: public, max-age=2628000" \
                    --data-binary "@$file_path" \
                    "$uri" > /dev/null
            fi
        done
    fi

    log_success "Assets processed successfully"
}

#######################################
# Step 10: Start and Monitor Deployment
#######################################

start_and_monitor_deployment() {
    log_info "Starting deployment..."

    api_request "POST" "/deployments/$DEPLOYMENT_ID/start" > /dev/null

    log_info "Monitoring deployment progress..."

    local status="pending"
    local timeout=60
    local elapsed=0

    # Wait for status to change from pending
    while [[ "$status" == "pending" ]] && [[ $elapsed -lt $timeout ]]; do
        sleep 1
        ((elapsed++))

        local response
        response=$(api_request "GET" "/deployments/$DEPLOYMENT_ID")
        status=$(echo "$response" | jq -r '.status')
    done

    if [[ "$status" == "pending" ]]; then
        die "Timeout waiting for deployment to start"
    fi

    if [[ "$status" == "failed" ]]; then
        local error_msg
        error_msg=$(echo "$response" | jq -r '.failed_message // "Unknown error"')
        die "Deployment failed: $error_msg"
    fi

    # Monitor deployment steps
    local step_timeout=600
    local current_step=""

    while [[ "$status" == "in_progress" || "$status" == "starting" ]]; do
        local response
        response=$(api_request "GET" "/deployments/$DEPLOYMENT_ID")
        status=$(echo "$response" | jq -r '.status')

        # Display current step
        local steps
        steps=$(echo "$response" | jq -c '.steps[]?')

        echo "$steps" | while read -r step; do
            local task=$(echo "$step" | jq -r '.task')
            local step_status=$(echo "$step" | jq -r '.status')

            case "$step_status" in
                in_progress)
                    if [[ "$current_step" != "$task" ]]; then
                        current_step="$task"
                        echo -n "  → $task..."
                    fi
                    ;;
                finished)
                    if [[ "$current_step" == "$task" ]]; then
                        echo " done"
                        current_step=""
                    fi
                    ;;
                failed)
                    echo " FAILED"
                    ;;
            esac
        done

        if [[ "$status" == "failed" ]]; then
            local error_msg
            error_msg=$(echo "$response" | jq -r '.failed_message // "Unknown error"')
            die "Deployment failed: $error_msg"
        fi

        if [[ "$status" == "cancelled" ]]; then
            die "Deployment was cancelled"
        fi

        sleep 1
    done

    if [[ "$status" == "finished" ]]; then
        log_success "Deployment completed successfully!"
    else
        die "Deployment ended with unexpected status: $status"
    fi
}

#######################################
# Step 11: Post-Deployment Checks
#######################################

post_deployment_checks() {
    log_info "Running post-deployment checks..."

    # Get environment URL
    local response
    response=$(api_request "GET" "/projects/$PROJECT_ID/environments/$ENVIRONMENT")

    local vanity_domain
    vanity_domain=$(echo "$response" | jq -r '.vanity_domain_name // empty')

    local custom_domain
    custom_domain=$(yq ".environments.$ENVIRONMENT.domain // empty" "$PROJECT_CONFIG")

    if [[ -n "$vanity_domain" ]]; then
        log_success "Environment URL: https://$vanity_domain"
    fi

    # Check for unmanaged domains
    local deployment_response
    deployment_response=$(api_request "GET" "/deployments/$DEPLOYMENT_ID")

    local unmanaged_domains
    unmanaged_domains=$(echo "$deployment_response" | jq -r '.unmanaged_domains[]? // empty')

    if [[ -n "$unmanaged_domains" ]]; then
        log_warning "The following domains require DNS configuration:"
        echo ""
        printf "%-40s %-10s %-s\n" "DOMAIN" "TYPE" "VALUE"
        printf "%-40s %-10s %-s\n" "------" "----" "-----"

        echo "$unmanaged_domains" | while read -r domain; do
            if [[ -n "$domain" ]]; then
                printf "%-40s %-10s %-s\n" "$domain" "CNAME" "$vanity_domain"
            fi
        done
        echo ""
    fi

    # Email domain warning
    if [[ -z "$custom_domain" || "$custom_domain" == "null" ]]; then
        log_warning "No domain configured. Email sending requires a verified domain."
        log_info "Run 'ymir email-identity:create' to configure email sending."
    fi
}

#######################################
# Main Execution
#######################################

main() {
    echo ""
    echo "======================================"
    echo "  Ymir Project Deploy Script"
    echo "======================================"
    echo ""

    # Check dependencies
    for cmd in curl jq yq zip; do
        if ! command -v "$cmd" &> /dev/null; then
            die "Required command not found: $cmd"
        fi
    done

    # Execute deployment steps
    parse_project_config
    echo ""

    generate_assets_hash
    echo ""

    build_project
    echo ""

    validate_project
    echo ""

    create_deployment
    echo ""

    upload_function_code
    echo ""

    process_assets
    echo ""

    start_and_monitor_deployment
    echo ""

    post_deployment_checks
    echo ""

    echo "======================================"
    log_success "Deployment to '$ENVIRONMENT' complete!"
    echo "======================================"
}

# Handle interrupts
trap 'echo ""; log_warning "Deployment interrupted"; exit 130' INT TERM

# Run main function
main
