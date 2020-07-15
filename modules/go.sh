# do a fresh login before running this to avoid issues from
# modules already loaded

./package-module.sh  ${2:-$(date +'%Y%m%d')}
#./package-module.sh  dev_${2:-$(date +'%Y%m%d')}
