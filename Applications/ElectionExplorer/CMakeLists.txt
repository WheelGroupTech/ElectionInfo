# Election Explorer: macOS / Linux build of the engine and its console tests.
#
# Windows builds keep using Applications.sln (ElectionExplorer.vcxproj, MSIX packaging);
# this file is only for macOS and Linux. It builds
#   ee_engine   - the engine (all of src/ except the Win32 UI main.c, ocr_win.c and
#                 settings.c) with the POSIX platform layer in src/platform/posix/ and OCR
#                 through Apple Vision (macOS, src/platform/macos/) or Tesseract (Linux, when
#                 installed; src/platform/linux/)
#   smoke_load  - the smoke suite (test/smoke_load.c), run from this folder
#   data_check  - the loader harness used by test/backup_data.py (test/data_check.c)
#
# Build (output goes under Applications/Build/<os>-<arch>-<config>/ElectionExplorer/, as
# AGENTS.md requires; see CMakePresets.json):
#   cmake --preset linux-x64-debug
#   cmake --build --preset linux-x64-debug
#   ctest --preset linux-x64-debug
cmake_minimum_required(VERSION 3.20)
project(ElectionExplorer LANGUAGES C)

if(WIN32)
    message(FATAL_ERROR "On Windows build Election Explorer with Applications.sln; "
                        "this CMake build is for macOS and Linux.")
endif()

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Debug)
endif()

# Output folder: Applications/Build/<os>-<arch>-<config>/ElectionExplorer/
if(APPLE)
    set(EE_OS mac)
else()
    set(EE_OS linux)
endif()
if(CMAKE_OSX_ARCHITECTURES MATCHES ";")
    set(EE_ARCH universal)
elseif(CMAKE_OSX_ARCHITECTURES)
    set(EE_ARCH ${CMAKE_OSX_ARCHITECTURES})
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|AMD64|amd64)$")
    set(EE_ARCH x64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64|ARM64)$")
    set(EE_ARCH arm64)
else()
    set(EE_ARCH ${CMAKE_SYSTEM_PROCESSOR})
endif()
string(TOLOWER "${CMAKE_BUILD_TYPE}" EE_CONFIG)
get_filename_component(EE_APPS_DIR "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)
set(EE_OUT_DIR "${EE_APPS_DIR}/Build/${EE_OS}-${EE_ARCH}-${EE_CONFIG}/ElectionExplorer")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${EE_OUT_DIR}")
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY "${EE_OUT_DIR}/lib")

set(EE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(EE_POSIX "${EE_SRC}/platform/posix")

add_library(ee_engine STATIC
    ${EE_SRC}/csv_sheet.c
    ${EE_SRC}/dominion_cvr.c
    ${EE_SRC}/ee_audit.c
    ${EE_SRC}/ee_audit_hart.c
    ${EE_SRC}/ee_cvr.c
    ${EE_SRC}/ee_glyph_ocr.c
    ${EE_SRC}/ee_rcv.c
    ${EE_SRC}/ee_results.c
    ${EE_SRC}/ee_results_dom.c
    ${EE_SRC}/ee_results_ocr.c
    ${EE_SRC}/ee_results_pdf.c
    ${EE_SRC}/filter.c
    ${EE_SRC}/hart_cvr.c
    ${EE_SRC}/hart_hvs.c
    ${EE_SRC}/hart_ocr.c
    ${EE_SRC}/pdf_reader.c
    ${EE_SRC}/voter_roster.c
    ${EE_SRC}/voter_table.c
    ${EE_SRC}/xls.c
    ${EE_SRC}/xlsx.c
    ${EE_SRC}/third_party/miniz/miniz.c
    ${EE_POSIX}/ee_port.c
    ${EE_POSIX}/settings_posix.c
)
# OCR: ocr_bridge.c implements ocr_win.h on top of an engine's plain C interface
# (src/platform/posix/ocr_engine.h). The engine files never see the stand-in windows.h, whose
# BOOL and macros clash with Objective-C's and Leptonica's.
option(EE_WITH_TESSERACT "Linux: read scanned documents with Tesseract when it is installed" ON)
if(APPLE)
    # Apple Vision (Objective-C)
    enable_language(OBJC)
    set(EE_MACOS "${EE_SRC}/platform/macos")
    target_sources(ee_engine PRIVATE ${EE_POSIX}/ocr_bridge.c ${EE_MACOS}/ocr_vision.m)
    target_include_directories(ee_engine PRIVATE ${EE_POSIX})
    set_source_files_properties(${EE_MACOS}/ocr_vision.m PROPERTIES COMPILE_OPTIONS "-fobjc-arc")
    target_link_libraries(ee_engine PUBLIC
        "-framework Vision" "-framework CoreGraphics" "-framework ImageIO" "-framework Foundation")
else()
    if(EE_WITH_TESSERACT)
        find_package(PkgConfig QUIET)
        if(PKG_CONFIG_FOUND)
            pkg_check_modules(EE_TESS QUIET tesseract lept)
        endif()
    endif()
    if(EE_TESS_FOUND)
        # Tesseract and Leptonica (Ubuntu: libtesseract-dev libleptonica-dev tesseract-ocr-eng)
        message(STATUS "OCR: Tesseract ${EE_TESS_tesseract_VERSION}")
        target_sources(ee_engine PRIVATE ${EE_POSIX}/ocr_bridge.c ${EE_SRC}/platform/linux/ocr_tesseract.c)
        target_include_directories(ee_engine PRIVATE ${EE_POSIX} ${EE_TESS_INCLUDE_DIRS})
        target_link_libraries(ee_engine PUBLIC ${EE_TESS_LINK_LIBRARIES})
    else()
        # no OCR engine: scanned documents are refused with a message
        message(STATUS "OCR: none (install Tesseract for scanned documents)")
        target_sources(ee_engine PRIVATE ${EE_POSIX}/ocr_none.c)
    endif()
endif()
# The stand-in windows.h / strsafe.h / intsafe.h come first, ahead of the system headers.
target_include_directories(ee_engine BEFORE PUBLIC ${EE_POSIX}/include)
target_include_directories(ee_engine PUBLIC ${EE_SRC})
target_compile_definitions(ee_engine PUBLIC
    UNICODE _UNICODE
    _DEFAULT_SOURCE            # glibc: POSIX functions alongside -std=c11
    _DARWIN_C_SOURCE           # macOS: the same
    _FILE_OFFSET_BITS=64       # 64-bit off_t for fseeko / ftello
)
target_compile_options(ee_engine PUBLIC
    -fno-strict-aliasing
    -Werror=implicit-function-declaration # a Win32 call the platform layer lacks
    -Wall -Wno-unknown-pragmas # #pragma comment(lib, ...) is MSVC's
    -Wno-unused-function -Wno-missing-braces -Wno-sign-compare
)
find_library(EE_LIBM m)
if(EE_LIBM)
    target_link_libraries(ee_engine PUBLIC ${EE_LIBM})
endif()

add_executable(smoke_load test/smoke_load.c ${EE_POSIX}/wmain_posix.c)
target_link_libraries(smoke_load PRIVATE ee_engine)
if(APPLE)
    # the OCR tests' page images and glyph outlines, drawn with CoreGraphics / CoreText in place
    # of GDI (test/tdraw_apple.c; plain C behind test/tdraw.h)
    target_sources(smoke_load PRIVATE test/tdraw_apple.c)
    target_link_libraries(smoke_load PRIVATE "-framework CoreText")
endif()

add_executable(data_check test/data_check.c ${EE_POSIX}/wmain_posix.c)
target_link_libraries(data_check PRIVATE ee_engine)

enable_testing()
add_test(NAME smoke_load COMMAND smoke_load WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})
