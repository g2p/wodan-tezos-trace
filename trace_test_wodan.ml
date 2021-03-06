let path = Sys.argv.(1)

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

(* Block is from mirage-block-unix *)
(* https://g2p.github.io/wodan/doc/wodan-for-mirage.html#9 *)
module Block = struct
  include Wodan.BlockCompat(Block)
  let connect name = Block.connect name
end

module Stor = Wodan.Make(Block)(struct
  include Wodan.StandardSuperblockParams
  let key_size = 64
  let block_size = 32*1024
end)

let flush_period = 1024

let v () =
  let root = "wodan.img" in
  let%lwt disk = Block.connect root in
  let%lwt info = Block.get_info disk in
  let%lwt () = Nocrypto_entropy_lwt.initialize () in
  let%lwt (stor, _gen) = Stor.prepare_io
    (Wodan.FormatEmptyDevice Int64.(div (mul info.size_sectors @@ of_int info.sector_size) @@ of_int Stor.P.block_size)) disk
    {Wodan.standard_mount_options with cache_size=3*32*1024; relax = { magic_crc = true; magic_crc_write = true}}
  in Lwt.return stor

let f = open_in_bin path

let t1 = Unix.gettimeofday ()

let%lwt () =
  Sys.catch_break true;
  Logs.set_reporter @@ Logs.format_reporter ();
  Logs.set_level @@ Some Logs.Info;
  let%lwt stor = v () in
  let mkl = ref 0 in
  let mvl = ref 0 in
  let flush_req_counter = ref 0 in
  let rec loop () =
    let elt = read_one f in
    match elt with
    | Read k ->
        let%lwt b = Stor.lookup stor @@ Stor.key_of_string_padded k in
        (match b with
         | Some _b -> ()
         | None -> failwith (Printf.sprintf "Missing key %s" k)
        );
        loop ()
    | Mem k ->
        let%lwt b = Stor.mem stor @@ Stor.key_of_string_padded k in
        assert b;
        loop ()
    | Write (k, l) ->
        let mkl' = String.length k in
        if mkl' > !mkl then mkl := mkl';
        if l > !mvl then mvl := l;
        let%lwt () = Stor.insert stor (Stor.key_of_string_padded k) (Stor.value_of_string @@ String.make l '\000') in
        loop ()
    | Commit ->
        flush_req_counter := succ !flush_req_counter;
        if !flush_req_counter mod flush_period = 0 then
          let%lwt _gen = Stor.flush stor in
          loop ()
        else
          loop ()
  in
  try
    loop ()
  with
  |End_of_file ->
      Printf.printf "Max key length %d\n" !mkl;
      Printf.printf "Max value length %d\n" !mvl;
      Lwt.return_unit
  |Sys.Break ->
      Printf.printf "Max key length %d\n" !mkl;
      Printf.printf "Max value length %d\n" !mvl;
      Lwt.return_unit

let t2 = Unix.gettimeofday ()
let () = Printf.printf "TOTAL %0.2f\n%!" (t2 -. t1);
