function exptools_key_bindings
  # Some part of code is from https://github.com/junegunn/fzf/blob/master/shell/key-bindings.fish (MIT License)

  function __fzfcmd
    set -q FZF_TMUX; or set FZF_TMUX 0
    set -q FZF_TMUX_HEIGHT; or set FZF_TMUX_HEIGHT 40%
    if [ $FZF_TMUX -eq 1 ]
      echo "fzf-tmux -d$FZF_TMUX_HEIGHT"
    else
      echo "fzf"
    end
  end

  function __etc_status_cmd
    set -q ETC_CMD; or set ETC_CMD "etc"
    set -q ETC_COMMON_OPTS; or set ETC_COMMON_OPTS ""
    set -q ETC_STATUS_OPTS; or set ETC_STATUS_OPTS "status"
    echo "$ETC_CMD $ETC_COMMON_OPTS $ETC_STATUS_OPTS"
  end

  function __etc_du_cmd
    set -q ETC_CMD; or set ETC_CMD "etc"
    set -q ETC_COMMON_OPTS; or set ETC_COMMON_OPTS ""
    set -q ETC_DU_OPTS; or set ETC_DU_OPTS "select all : du"
    echo "$ETC_CMD $ETC_COMMON_OPTS $ETC_DU_OPTS"
  end

  function fzf-job-id-widget -d "Insert selected jobs' job ID"
    set -q FZF_TMUX_HEIGHT; or set FZF_TMUX_HEIGHT 40%
    set -lx FZF_DEFAULT_OPTS "--height $FZF_TMUX_HEIGHT --ansi --reverse --no-sort $FZF_DEFAULT_OPTS"
    eval (__etc_status_cmd) | eval (__fzfcmd) --multi | while read -l r; set result $result $r; end

    for i in $result
      commandline -it -- (string replace -r '^   (j-\S+) .*$' '$1' $i)
      commandline -it -- ' '
    end
    commandline -f repaint
  end

  function fzf-param-id-widget -d "Insert selected parameters' parameter ID"
    set -q FZF_TMUX_HEIGHT; or set FZF_TMUX_HEIGHT 40%
    set -lx FZF_DEFAULT_OPTS "--height $FZF_TMUX_HEIGHT $FZF_DEFAULT_OPTS"
    eval (__etc_du_cmd) | eval (__fzfcmd) --multi | while read -l r; set result $result $r; end

    for i in $result
      commandline -it -- (string replace -r '^(p-\S+) .*$' '$1' $i)
      commandline -it -- ' '
    end
    commandline -f repaint
  end

  function fzf-hash-id-widget -d "Insert selected parameters' hash ID"
    set -q FZF_TMUX_HEIGHT; or set FZF_TMUX_HEIGHT 40%
    set -lx FZF_DEFAULT_OPTS "--height $FZF_TMUX_HEIGHT $FZF_DEFAULT_OPTS"
    eval (__etc_du_cmd) | eval (__fzfcmd) --multi | while read -l r; set result $result $r; end

    for i in $result
      commandline -it -- (string replace -r '^\S+ (h-\S+) .*$' '$1' $i)
      commandline -it -- ' '
    end
    commandline -f repaint
  end

  bind \e8 fzf-job-id-widget
  bind \e9 fzf-param-id-widget
  bind \e0 fzf-hash-id-widget

  if bind -M insert > /dev/null 2>&1
    bind -M insert \e8 fzf-job-id-widget
    bind -M insert \e9 fzf-param-id-widget
    bind -M insert \e0 fzf-hash-id-widget
  end
end
