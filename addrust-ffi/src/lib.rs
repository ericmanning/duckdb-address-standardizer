use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use addrust::config::Config;
use addrust::pipeline::Pipeline;

/// Opaque pipeline handle.
pub struct AddrstPipeline(Pipeline);

/// Parsed address result — flat C struct with nullable C strings.
/// NULL pointer means the field was not extracted.
#[repr(C)]
pub struct AddrstAddress {
    pub street_number: *mut c_char,
    pub pre_direction: *mut c_char,
    pub street_name: *mut c_char,
    pub suffix: *mut c_char,
    pub post_direction: *mut c_char,
    pub unit_type: *mut c_char,
    pub unit: *mut c_char,
    pub po_box: *mut c_char,
    pub building: *mut c_char,
    pub building_type: *mut c_char,
    pub extra_front: *mut c_char,
    pub extra_back: *mut c_char,
    pub city: *mut c_char,
    pub state: *mut c_char,
    pub zip: *mut c_char,
}

fn option_to_c(opt: &Option<String>) -> *mut c_char {
    match opt {
        Some(s) if !s.is_empty() => {
            CString::new(s.as_str())
                .map(|cs| cs.into_raw())
                .unwrap_or(ptr::null_mut())
        }
        _ => ptr::null_mut(),
    }
}

/// # Safety
/// `ptr` must have been allocated by `CString::into_raw()` or be null.
unsafe fn free_c_string(ptr: *mut c_char) {
    if !ptr.is_null() {
        unsafe { drop(CString::from_raw(ptr)); }
    }
}

/// Create a default pipeline.
/// Caller must free with `addrust_pipeline_free()`.
#[unsafe(no_mangle)]
pub extern "C" fn addrust_pipeline_new() -> *mut AddrstPipeline {
    let pipeline = Pipeline::default();
    Box::into_raw(Box::new(AddrstPipeline(pipeline)))
}

/// Create a pipeline from a TOML config file path.
/// Returns the default pipeline if `path` is NULL.
/// Returns NULL only on UTF-8 conversion error.
/// Caller must free with `addrust_pipeline_free()`.
///
/// # Safety
/// `path` must be a valid null-terminated C string or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn addrust_pipeline_new_from_file(
    path: *const c_char,
) -> *mut AddrstPipeline {
    if path.is_null() {
        return addrust_pipeline_new();
    }
    let path_str = match unsafe { CStr::from_ptr(path) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let config = Config::load(std::path::Path::new(path_str));
    let pipeline = Pipeline::from_config(&config);
    Box::into_raw(Box::new(AddrstPipeline(pipeline)))
}

/// Parse a single address string.
/// Returns NULL if `pipeline` or `input` is NULL, or on UTF-8 error.
/// Caller must free the result with `addrust_address_free()`.
///
/// # Safety
/// `pipeline` must be a valid pointer from `addrust_pipeline_new*()` or NULL.
/// `input` must be a valid null-terminated C string or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn addrust_parse(
    pipeline: *const AddrstPipeline,
    input: *const c_char,
) -> *mut AddrstAddress {
    if pipeline.is_null() || input.is_null() {
        return ptr::null_mut();
    }
    let input_str = match unsafe { CStr::from_ptr(input) }.to_str() {
        Ok(s) => s,
        Err(_) => return ptr::null_mut(),
    };
    let addr = unsafe { &*pipeline }.0.parse(input_str);

    let result = Box::new(AddrstAddress {
        street_number: option_to_c(&addr.street_number),
        pre_direction: option_to_c(&addr.pre_direction),
        street_name: option_to_c(&addr.street_name),
        suffix: option_to_c(&addr.suffix),
        post_direction: option_to_c(&addr.post_direction),
        unit_type: option_to_c(&addr.unit_type),
        unit: option_to_c(&addr.unit),
        po_box: option_to_c(&addr.po_box),
        building: option_to_c(&addr.building),
        building_type: option_to_c(&addr.building_type),
        extra_front: option_to_c(&addr.extra_front),
        extra_back: option_to_c(&addr.extra_back),
        city: option_to_c(&addr.city),
        state: option_to_c(&addr.state),
        zip: option_to_c(&addr.zip),
    });
    Box::into_raw(result)
}

/// Free a parsed address returned by `addrust_parse()`.
///
/// # Safety
/// `addr` must be a valid pointer from `addrust_parse()` or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn addrust_address_free(addr: *mut AddrstAddress) {
    if addr.is_null() {
        return;
    }
    let a = unsafe { Box::from_raw(addr) };
    unsafe {
        free_c_string(a.street_number);
        free_c_string(a.pre_direction);
        free_c_string(a.street_name);
        free_c_string(a.suffix);
        free_c_string(a.post_direction);
        free_c_string(a.unit_type);
        free_c_string(a.unit);
        free_c_string(a.po_box);
        free_c_string(a.building);
        free_c_string(a.building_type);
        free_c_string(a.extra_front);
        free_c_string(a.extra_back);
        free_c_string(a.city);
        free_c_string(a.state);
        free_c_string(a.zip);
    }
}

/// Free a pipeline returned by `addrust_pipeline_new()` or
/// `addrust_pipeline_new_from_file()`.
///
/// # Safety
/// `pipeline` must be a valid pointer from `addrust_pipeline_new*()` or NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn addrust_pipeline_free(pipeline: *mut AddrstPipeline) {
    if !pipeline.is_null() {
        unsafe { drop(Box::from_raw(pipeline)); }
    }
}
