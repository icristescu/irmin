Running stat on a layered store after a first freeze
  $ PACK_LAYERED=true ../irmin_fsck.exe stat ../data/layered_pack_upper
  >> Getting statistics for store: `../data/layered_pack_upper'
  
  	0k contents / 0k nodes / 0k commits	0k contents / 0k nodes / 0k commits
  	0k contents / 0k nodes / 0k commits	0k contents / 0k nodes / 0k commits
  	0k contents / 0k nodes / 0k commits	0k contents / 0k nodes / 0k commits
  {
    "hash_size": {
      "Bytes": 64
    },
    "log_size": 500000,
    "files": {
      "flip": "Upper0",
      "lower": {
        "pack": {
          "size": {
            "Bytes": 466
          },
          "offset": 442,
          "generation": 0,
          "version": "V2"
        },
        "branch": {
          "size": {
            "Bytes": 24
          },
          "offset": 0,
          "generation": 0,
          "version": "V2"
        },
        "dict": {
          "size": {
            "Bytes": 24
          },
          "offset": 0,
          "generation": 0,
          "version": "V2"
        }
      },
      "upper1": {
        "pack": {
          "size": {
            "Bytes": 24
          },
          "offset": 0,
          "generation": 1,
          "version": "V2"
        },
        "branch": {
          "size": {
            "Bytes": 24
          },
          "offset": 0,
          "generation": 0,
          "version": "V2"
        },
        "dict": {
          "size": {
            "Bytes": 24
          },
          "offset": 0,
          "generation": 0,
          "version": "V2"
        }
      },
      "upper0": {
        "pack": {
          "size": {
            "Bytes": 530
          },
          "offset": 506,
          "generation": 0,
          "version": "V2"
        },
        "branch": {
          "size": {
            "Bytes": 98
          },
          "offset": 74,
          "generation": 0,
          "version": "V2"
        },
        "dict": {
          "size": {
            "Bytes": 24
          },
          "offset": 0,
          "generation": 0,
          "version": "V2"
        }
      }
    },
    "objects": {
      "lower": {
        "nb_commits": 1,
        "nb_nodes": 3,
        "nb_contents": 1
      },
      "upper1": {
        "nb_commits": 0,
        "nb_nodes": 0,
        "nb_contents": 0
      },
      "upper0": {
        "nb_commits": 1,
        "nb_nodes": 3,
        "nb_contents": 1
      }
    }
  }

Running check on a layered store that is not self contained

  $ PACK_LAYERED=true ../irmin_fsck.exe check ../data/layered_pack_upper
  >> Ok -- Upper layer is self contained for heads 9ebb8857c8705ffdc89542ce4a7e242a0544aec1c9ad5182d344a2e90b27096e3c97a0ef7683801a12e803729ed244e6d1737dff85eb22fe5aafe91364cd2457
