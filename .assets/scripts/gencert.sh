#!/bin/bash

# https://stackoverflow.com/questions/64814173/how-do-i-use-sans-with-openssl-instead-of-common-name/66980106#66980106

here=$(dirname $0)
subj="/C=US/ST=Washington/L=Richland/O=PNNL/CN=Local CA"
outdir="$here/../"
domain="localhost"

openssl genrsa -out $outdir/ca.key 2048
openssl req -new -x509 -days 365 -key $outdir/ca.key \
  -subj "$subj" \
  -out $outdir/ca.crt

openssl req -newkey rsa:2048 -nodes \
  -keyout $outdir/$domain.key \
  -subj "$subj" \
  -out $outdir/$domain.csr
openssl x509 -req \
  -extfile <(printf "subjectAltName=DNS:$domain") \
  -days 365 \
  -in $outdir/$domain.csr \
  -CA $outdir/ca.crt \
  -CAkey $outdir/ca.key \
  -CAcreateserial \
  -out $outdir/$domain.crt

rm $outdir/ca.srl
rm $outdir/$domain.csr