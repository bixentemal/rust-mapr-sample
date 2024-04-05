use std::ffi::CString;
use std::sync::{Arc, Mutex, Condvar};
use std::{ptr, slice};

pub type hb_connection_t = *mut ::std::os::raw::c_void;
pub type hb_client_t = *mut ::std::os::raw::c_void;

pub type hb_scanner_t = *mut ::std::os::raw::c_void;

pub type hb_result_t = *mut ::std::os::raw::c_void;

pub type byte_t = u8;

#[doc = " Base HBase Cell type."]
#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct hb_cell_type {
    pub row: *mut byte_t,
    pub row_len: usize,
    pub family: *mut byte_t,
    pub family_len: usize,
    pub qualifier: *mut byte_t,
    pub qualifier_len: usize,
    pub value: *mut byte_t,
    pub value_len: usize,
    pub ts: i64,
    pub flags_: i64,
    pub private_: *mut ::std::os::raw::c_void,
}

pub type hb_cell_t = hb_cell_type;

#[doc = " Client disconnection callback typedef\n This callback is triggered after the connections are closed, but just before\n the client is freed.\n\n Refer to the section on error code for the list of possible values\n for 'err'. A value of 0 indicates success."]
pub type hb_client_disconnection_cb = ::std::option::Option<
    unsafe extern "C" fn(err: i32, client: hb_client_t, extra: *mut ::std::os::raw::c_void),
>;

#[doc = " Scanner callback typedef\n This callback is triggered when scanner next returns results.\n\n The individual results in the results array must be freed by calling\n hb_result_destroy(). The array itself is freed once the callback returns.\n\n Refer to the section on error code for the list of possible values\n for 'err'. A value of 0 indicates success."]
pub type hb_scanner_cb = ::std::option::Option<
    unsafe extern "C" fn(
        err: i32,
        scanner: hb_scanner_t,
        results: *mut hb_result_t,
        num_results: usize,
        extra: *mut ::std::os::raw::c_void,
    ),
>;

extern "C" {
    #[doc = " Creates an hb_connection_t instance and initializes its address into\n the passed pointer."]
    pub fn hb_connection_create(
        zk_quorum: *const ::std::os::raw::c_char,
        zk_root: *const ::std::os::raw::c_char,
        connection_ptr: *mut hb_connection_t,
    ) -> i32;

    #[doc = " Initializes a handle to hb_client_t which can be passed to other HBase APIs.\n You need to use this method only once per HBase cluster. The returned handle\n is thread safe.\n\n @returns 0 on success, non-zero error code in case of failure."]
    pub fn hb_client_create(connection: hb_connection_t, client_ptr: *mut hb_client_t) -> i32;

    #[doc = " Creates a client side row scanner. The returned scanner is not thread safe.\n No RPC will be invoked until the call to fetch the next set of rows is made.\n You can set the various attributes of this scanner until that point.\n @returns 0 on success, non-zero error code in case of failure."]
    pub fn hb_scanner_create(client: hb_client_t, scanner_ptr: *mut hb_scanner_t) -> i32;

    #[doc = " Cleans up hb_client_t handle and release any held resources.\n The callback is called after the connections are closed, but just before the\n client is freed."]
    pub fn hb_client_destroy(
        client: hb_client_t,
        cb: hb_client_disconnection_cb,
        extra: *mut ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Set the table name for the scanner"]
    pub fn hb_scanner_set_table(
        scanner: hb_scanner_t,
        table: *const ::std::os::raw::c_char,
        table_length: usize,
    ) -> i32;

    #[doc = " Sets the maximum number of rows to scan per call to hb_scanner_next()."]
    pub fn hb_scanner_set_num_max_rows(scanner: hb_scanner_t, cache_size: usize) -> i32;

    #[doc = " Sets the maximum versions of a column to fetch."]
    pub fn hb_scanner_set_num_versions(scanner: hb_scanner_t, num_versions: i8) -> i32;

    #[doc = " Optional. Adds a filter to the hb_scanner_t object.\n\n The filter must be specified using HBase Filter Language.\n Refer to class org.apache.hadoop.hbase.filter.ParseFilter and\n https://issues.apache.org/jira/browse/HBASE-4176 or\n http://hbase.apache.org/book.html#thrift.filter-language for\n language syntax and additional details."]
    pub fn hb_scanner_set_filter(
        scanner: hb_scanner_t,
        filter: *const byte_t,
        filterLen: i32,
    ) -> i32;

    #[doc = " Returns the row key of this hb_result_t object.\n This buffer is valid until hb_result_destroy() is called.\n Callers should not modify this buffer."]
    pub fn hb_result_get_key(
        result: hb_result_t,
        key_ptr: *mut *const byte_t,
        key_length_ptr: *mut usize,
    ) -> i32;

    #[doc = " Returns the array of pointers to constant hb_cell_t structures with the cells\n of the result. The buffers are valid until hb_result_destroy() is called. The\n variable pointed by num_cells_ptr is set to the number of cells in the result.\n\n Calling this function multiple times for the same hb_result_t may return\n the same buffers. Callers should not modify these buffers."]
    pub fn hb_result_get_cells(
        result: hb_result_t,
        cells_ptr: *mut *mut *const hb_cell_t,
        num_cells_ptr: *mut usize,
    ) -> i32;

    #[doc = " Returns the total number of cells in this hb_result_t object."]
    pub fn hb_result_get_cell_count(result: hb_result_t, cell_count_ptr: *mut usize) -> i32;

    #[doc = " Frees any resources held by the hb_result_t object."]
    pub fn hb_result_destroy(result: hb_result_t) -> i32;

    #[doc = " Request the next set of results from the server. You can set the maximum\n number of rows returned by this call using hb_scanner_set_num_max_rows()."]
    pub fn hb_scanner_next(
        scanner: hb_scanner_t,
        cb: fn(i32, hb_scanner_t, *mut hb_result_t, usize, *mut ::std::os::raw::c_void),
        extra: *mut ::std::os::raw::c_void,
    ) -> i32;

    #[doc = " Destroy the connection and free all resources allocated at creation time."]
    pub fn hb_connection_destroy(connection: hb_connection_t) -> i32;
}

static mut count: i32 = 0;

struct Synchronizer {
    pair: Arc<(Mutex<bool>, Condvar)>
}

fn print_row(result: hb_result_t) {

    // fetch key
    let mut key: *const byte_t = ptr::null();
    let mut key_size: usize = 0;
    let ret = unsafe { hb_result_get_key(result, &mut key, &mut key_size) };

    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    assert!(!key.is_null());
    assert!(key_size >= 0);

    let keystr_as_vector = unsafe { slice::from_raw_parts(key, key_size) }.to_vec();
    let keystr = std::str::from_utf8(&keystr_as_vector).unwrap();

    let mut cell_count: usize = 0;
    let ret = unsafe { hb_result_get_cell_count(result, &mut cell_count) };
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    println!("Row {},cell count {}", keystr, cell_count);

    let mut voidcell: *const hb_cell_t = ptr::null();
    let mut cells: *mut *const hb_cell_t = &mut voidcell;
    let ret = unsafe { hb_result_get_cells(result, &mut cells, &mut cell_count) };
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }
    for i in 0..cell_count {
        let c: *const hb_cell_t = unsafe { *cells.offset(i as isize) };
        let familystr_as_vector = unsafe { slice::from_raw_parts((*c).family, (*c).family_len) }.to_vec();
        let familystr = std::str::from_utf8(&familystr_as_vector).unwrap();

        let qualifierstr_as_vector = unsafe { slice::from_raw_parts((*c).qualifier, (*c).qualifier_len) }.to_vec();
        let qualifierstr = std::str::from_utf8(&qualifierstr_as_vector).unwrap();

        let value_as_vector = unsafe { slice::from_raw_parts((*c).value, (*c).value_len) }.to_vec();
        let valuestr = std::str::from_utf8(&value_as_vector).unwrap();

        let ts :i64 = unsafe {(*c).ts};

        println!("Cell {} \
        family={} \
        qualifier={} \
        value={} \
        timestamp={} \
        ", i, familystr, qualifierstr, valuestr, ts);
    }

}

fn scan_callback(err: i32, scanner: hb_scanner_t, results: *mut hb_result_t, num_results: usize, extra: *mut ::std::os::raw::c_void) {
    if num_results > 0 {
        for i in 0..num_results {
            unsafe {print_row(*results.offset(i as isize)); };
            unsafe { count = count+1; }
            unsafe { hb_result_destroy(*results.offset(i as isize)); }
        }
        unsafe {hb_scanner_next(scanner, scan_callback, extra); }
    } else {
        unsafe {
            println!("No result in callback, exiting...");
            let synchronizer_ptr: *const Synchronizer = extra as *const Synchronizer;

            let (lock, cvar) = &*(*synchronizer_ptr).pair;
            let mut done = lock.lock().unwrap();
            *done = true;
            // We notify the condvar that the value has changed.
            cvar.notify_one();
        }
    }

}

fn main() {
    let table_name = "/tmp/tempTable";
    let zk_quorum = "maprdemo.mapr.io";
    let zk_root:*const ::std::os::raw::c_char = ptr::null();
    let mut hb_cnx : hb_connection_t = ptr::null_mut();
    let mut hb_client: hb_client_t = ptr::null_mut();
    let mut hb_scanner: hb_scanner_t = ptr::null_mut();

    let pair: Arc<(Mutex<bool>, Condvar)> = Arc::new((Mutex::new(false), Condvar::new()));

    let synchronizer: Box<Synchronizer> = Box::new(Synchronizer {
        pair: Arc::clone(&pair)
    });

    let c_str_zk_quorum = CString::new(zk_quorum).unwrap();
    let c_char_pts_zk_quorum: *const ::std::os::raw::c_char = c_str_zk_quorum.as_ptr() as *const ::std::os::raw::c_char;
    let mut ret = unsafe { hb_connection_create(c_char_pts_zk_quorum, zk_root, &mut hb_cnx) };
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    unsafe { ret = hb_client_create(hb_cnx, &mut hb_client); }
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    let c_str_table_name = CString::new(table_name).unwrap();
    let table_name_len = table_name.chars().count();
    let c_char_pts_table_name: *const ::std::os::raw::c_char = c_str_table_name.as_ptr() as *const ::std::os::raw::c_char;

    unsafe { ret = hb_scanner_create(hb_client, &mut hb_scanner); }
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    unsafe { ret = hb_scanner_set_table(hb_scanner, c_char_pts_table_name, table_name_len); }
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    let synchronizer_ptr: *const Synchronizer = &*synchronizer;
    unsafe { ret = hb_scanner_next(hb_scanner, scan_callback, synchronizer_ptr as *mut ::std::os::raw::c_void); } // dispatch the call
    if ret > 0 {
        panic!("return code fail: {0}", ret);
    }

    println!("Wait for callback to exit.");

    unsafe {
        let (lock, cvar) = &*pair;
        let mut done = lock.lock().unwrap();
        while !*done {
            done = cvar.wait(done).unwrap();
        }
    }

    // destroy client - commented out since crash on macos due du bug in libMapRClient.dylib
    //hb_client_destroy(hb_client);

    // destroy connection - commented out since crash on macos due du bug in libMapRClient.dylib
    //hb_connection_destroy();

    println!("done!");
}
