let path = Sys.argv.(1)
let sync_flag =
  if Array.length Sys.argv > 2 then
    match String.lowercase_ascii Sys.argv.(2) with
    | "nosync" -> [ Lmdb.NoSync ]
    | "nometasync" -> [ Lmdb.NoMetaSync ]
    | s ->
        Format.eprintf "unknown %s@." s;
        exit 1
  else
    []

type key = string

type v =
  | Commit
  | Mem of key
  | Read of key
  | Write of key * int

let read_key ic =
  let len = input_byte ic in
  let v = really_input_string ic len in
  v

let read_int32 ic =
  let a1 = input_byte ic in
  let a2 = input_byte ic in
  let a3 = input_byte ic in
  let a4 = input_byte ic in
  (a1 lsl 24) +
  (a2 lsl 16) +
  (a3 lsl 8)  +
  a4

let read_one ic =
  let c = input_char ic in
  match c with
  | 'c' -> Commit
  | 'r' -> Read (read_key ic)
  | 'm' -> Mem (read_key ic)
  | 'w' ->
    let key_len = input_byte ic in
    let data_len = read_int32 ic in
    let key = really_input_string ic key_len in
    Write (key, data_len)
  | _ ->
    failwith (Printf.sprintf "Unknown action %c" c)

let v =
  let root = "./testmdb/" in
  let flags = Lmdb.NoRdAhead :: Lmdb.NoTLS :: sync_flag in
  let file_flags = 0o644 in
  let mapsize = 40_960_000_000L in
  let () = Unix.mkdir root 0o755 in
  match Lmdb.opendir ~mapsize ~flags root file_flags with
  | Error err ->
      failwith (Lmdb.string_of_error err)
  | Ok db -> db

let f = open_in_bin path

let assert_ok = function
  | Ok _ -> ()
  | Error err -> failwith (Lmdb.string_of_error err)

let ok = function
  | Ok v -> v
  | Error err -> failwith (Lmdb.string_of_error err)

let t1 = Unix.gettimeofday ()

let () =
  let txn = ok (Lmdb.create_rw_txn v) in
  let db = ok (Lmdb.opendb txn) in
  let t = Unix.gettimeofday () in
  let rec loop (txn, db, t) =
    let elt = read_one f in
    match elt with
    | Read k ->
        let b = ok (Lmdb.get txn db k) in
        let _v = Bigarray.Array1.get b 0 in
        loop (txn, db, t)
    | Mem k ->
        assert_ok (Lmdb.mem txn db k);
        loop (txn, db, t)
    | Write (k, l) ->
        assert_ok (Lmdb.put_string txn db k (String.make l '\000'));
        loop (txn, db, t)
    | Commit ->
        assert_ok (Lmdb.commit_txn txn);
        let t' = Unix.gettimeofday () in
        Printf.printf "%0.2f\n%!" (t' -. t);
        let txn = ok (Lmdb.create_rw_txn v) in
        let db = ok (Lmdb.opendb txn) in
        loop (txn, db, t')
  in
  try
    loop (txn, db, t)
  with End_of_file -> ()

let t2 = Unix.gettimeofday ()
let () = Printf.printf "TOTAL %0.2f\n%!" (t2 -. t1);
