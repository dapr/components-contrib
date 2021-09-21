// ------------------------------------------------------------
// Copyright (c) Microsoft Corporation and Dapr Contributors.
// Licensed under the MIT License.
// ------------------------------------------------------------

package cloudstate

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/golang/protobuf/ptypes"
	any "github.com/golang/protobuf/ptypes/any"
	empty "github.com/golang/protobuf/ptypes/empty"
	jsoniter "github.com/json-iterator/go"
	"google.golang.org/grpc"

	"github.com/dapr/components-contrib/state"
	pb "github.com/dapr/components-contrib/state/cloudstate/proto"
	kvstore_pb "github.com/dapr/components-contrib/state/cloudstate/proto/kv_store"
	"github.com/dapr/kit/logger"
)

const (
	host                    = "host"
	serverPort              = "serverPort"
	defaultOperationTimeout = time.Second * 10
)

//nolint:gochecknoglobals
var doOnce sync.Once

//nolint:gochecknoglobals
var fileDesc = `Ct0BChlnb29nbGUvcHJvdG9idWYvYW55LnByb3RvEg9nb29nbGUucHJvdG9idWYi
NgoDQW55EhkKCHR5cGVfdXJsGAEgASgJUgd0eXBlVXJsEhQKBXZhbHVlGAIgASgM
UgV2YWx1ZUJvChNjb20uZ29vZ2xlLnByb3RvYnVmQghBbnlQcm90b1ABWiVnaXRo
dWIuY29tL2dvbGFuZy9wcm90b2J1Zi9wdHlwZXMvYW55ogIDR1BCqgIeR29vZ2xl
LlByb3RvYnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMKtwEKG2dvb2dsZS9wcm90
b2J1Zi9lbXB0eS5wcm90bxIPZ29vZ2xlLnByb3RvYnVmIgcKBUVtcHR5QnYKE2Nv
bS5nb29nbGUucHJvdG9idWZCCkVtcHR5UHJvdG9QAVonZ2l0aHViLmNvbS9nb2xh
bmcvcHJvdG9idWYvcHR5cGVzL2VtcHR5+AEBogIDR1BCqgIeR29vZ2xlLlByb3Rv
YnVmLldlbGxLbm93blR5cGVzYgZwcm90bzMKmzsKIGdvb2dsZS9wcm90b2J1Zi9k
ZXNjcmlwdG9yLnByb3RvEg9nb29nbGUucHJvdG9idWYiTQoRRmlsZURlc2NyaXB0
b3JTZXQSOAoEZmlsZRgBIAMoCzIkLmdvb2dsZS5wcm90b2J1Zi5GaWxlRGVzY3Jp
cHRvclByb3RvUgRmaWxlIuQEChNGaWxlRGVzY3JpcHRvclByb3RvEhIKBG5hbWUY
ASABKAlSBG5hbWUSGAoHcGFja2FnZRgCIAEoCVIHcGFja2FnZRIeCgpkZXBlbmRl
bmN5GAMgAygJUgpkZXBlbmRlbmN5EisKEXB1YmxpY19kZXBlbmRlbmN5GAogAygF
UhBwdWJsaWNEZXBlbmRlbmN5EicKD3dlYWtfZGVwZW5kZW5jeRgLIAMoBVIOd2Vh
a0RlcGVuZGVuY3kSQwoMbWVzc2FnZV90eXBlGAQgAygLMiAuZ29vZ2xlLnByb3Rv
YnVmLkRlc2NyaXB0b3JQcm90b1ILbWVzc2FnZVR5cGUSQQoJZW51bV90eXBlGAUg
AygLMiQuZ29vZ2xlLnByb3RvYnVmLkVudW1EZXNjcmlwdG9yUHJvdG9SCGVudW1U
eXBlEkEKB3NlcnZpY2UYBiADKAsyJy5nb29nbGUucHJvdG9idWYuU2VydmljZURl
c2NyaXB0b3JQcm90b1IHc2VydmljZRJDCglleHRlbnNpb24YByADKAsyJS5nb29n
bGUucHJvdG9idWYuRmllbGREZXNjcmlwdG9yUHJvdG9SCWV4dGVuc2lvbhI2Cgdv
cHRpb25zGAggASgLMhwuZ29vZ2xlLnByb3RvYnVmLkZpbGVPcHRpb25zUgdvcHRp
b25zEkkKEHNvdXJjZV9jb2RlX2luZm8YCSABKAsyHy5nb29nbGUucHJvdG9idWYu
U291cmNlQ29kZUluZm9SDnNvdXJjZUNvZGVJbmZvEhYKBnN5bnRheBgMIAEoCVIG
c3ludGF4IrkGCg9EZXNjcmlwdG9yUHJvdG8SEgoEbmFtZRgBIAEoCVIEbmFtZRI7
CgVmaWVsZBgCIAMoCzIlLmdvb2dsZS5wcm90b2J1Zi5GaWVsZERlc2NyaXB0b3JQ
cm90b1IFZmllbGQSQwoJZXh0ZW5zaW9uGAYgAygLMiUuZ29vZ2xlLnByb3RvYnVm
LkZpZWxkRGVzY3JpcHRvclByb3RvUglleHRlbnNpb24SQQoLbmVzdGVkX3R5cGUY
AyADKAsyIC5nb29nbGUucHJvdG9idWYuRGVzY3JpcHRvclByb3RvUgpuZXN0ZWRU
eXBlEkEKCWVudW1fdHlwZRgEIAMoCzIkLmdvb2dsZS5wcm90b2J1Zi5FbnVtRGVz
Y3JpcHRvclByb3RvUghlbnVtVHlwZRJYCg9leHRlbnNpb25fcmFuZ2UYBSADKAsy
Ly5nb29nbGUucHJvdG9idWYuRGVzY3JpcHRvclByb3RvLkV4dGVuc2lvblJhbmdl
Ug5leHRlbnNpb25SYW5nZRJECgpvbmVvZl9kZWNsGAggAygLMiUuZ29vZ2xlLnBy
b3RvYnVmLk9uZW9mRGVzY3JpcHRvclByb3RvUglvbmVvZkRlY2wSOQoHb3B0aW9u
cxgHIAEoCzIfLmdvb2dsZS5wcm90b2J1Zi5NZXNzYWdlT3B0aW9uc1IHb3B0aW9u
cxJVCg5yZXNlcnZlZF9yYW5nZRgJIAMoCzIuLmdvb2dsZS5wcm90b2J1Zi5EZXNj
cmlwdG9yUHJvdG8uUmVzZXJ2ZWRSYW5nZVINcmVzZXJ2ZWRSYW5nZRIjCg1yZXNl
cnZlZF9uYW1lGAogAygJUgxyZXNlcnZlZE5hbWUaegoORXh0ZW5zaW9uUmFuZ2US
FAoFc3RhcnQYASABKAVSBXN0YXJ0EhAKA2VuZBgCIAEoBVIDZW5kEkAKB29wdGlv
bnMYAyABKAsyJi5nb29nbGUucHJvdG9idWYuRXh0ZW5zaW9uUmFuZ2VPcHRpb25z
UgdvcHRpb25zGjcKDVJlc2VydmVkUmFuZ2USFAoFc3RhcnQYASABKAVSBXN0YXJ0
EhAKA2VuZBgCIAEoBVIDZW5kInwKFUV4dGVuc2lvblJhbmdlT3B0aW9ucxJYChR1
bmludGVycHJldGVkX29wdGlvbhjnByADKAsyJC5nb29nbGUucHJvdG9idWYuVW5p
bnRlcnByZXRlZE9wdGlvblITdW5pbnRlcnByZXRlZE9wdGlvbioJCOgHEICAgIAC
IpgGChRGaWVsZERlc2NyaXB0b3JQcm90bxISCgRuYW1lGAEgASgJUgRuYW1lEhYK
Bm51bWJlchgDIAEoBVIGbnVtYmVyEkEKBWxhYmVsGAQgASgOMisuZ29vZ2xlLnBy
b3RvYnVmLkZpZWxkRGVzY3JpcHRvclByb3RvLkxhYmVsUgVsYWJlbBI+CgR0eXBl
GAUgASgOMiouZ29vZ2xlLnByb3RvYnVmLkZpZWxkRGVzY3JpcHRvclByb3RvLlR5
cGVSBHR5cGUSGwoJdHlwZV9uYW1lGAYgASgJUgh0eXBlTmFtZRIaCghleHRlbmRl
ZRgCIAEoCVIIZXh0ZW5kZWUSIwoNZGVmYXVsdF92YWx1ZRgHIAEoCVIMZGVmYXVs
dFZhbHVlEh8KC29uZW9mX2luZGV4GAkgASgFUgpvbmVvZkluZGV4EhsKCWpzb25f
bmFtZRgKIAEoCVIIanNvbk5hbWUSNwoHb3B0aW9ucxgIIAEoCzIdLmdvb2dsZS5w
cm90b2J1Zi5GaWVsZE9wdGlvbnNSB29wdGlvbnMitgIKBFR5cGUSDwoLVFlQRV9E
T1VCTEUQARIOCgpUWVBFX0ZMT0FUEAISDgoKVFlQRV9JTlQ2NBADEg8KC1RZUEVf
VUlOVDY0EAQSDgoKVFlQRV9JTlQzMhAFEhAKDFRZUEVfRklYRUQ2NBAGEhAKDFRZ
UEVfRklYRUQzMhAHEg0KCVRZUEVfQk9PTBAIEg8KC1RZUEVfU1RSSU5HEAkSDgoK
VFlQRV9HUk9VUBAKEhAKDFRZUEVfTUVTU0FHRRALEg4KClRZUEVfQllURVMQDBIP
CgtUWVBFX1VJTlQzMhANEg0KCVRZUEVfRU5VTRAOEhEKDVRZUEVfU0ZJWEVEMzIQ
DxIRCg1UWVBFX1NGSVhFRDY0EBASDwoLVFlQRV9TSU5UMzIQERIPCgtUWVBFX1NJ
TlQ2NBASIkMKBUxhYmVsEhIKDkxBQkVMX09QVElPTkFMEAESEgoOTEFCRUxfUkVR
VUlSRUQQAhISCg5MQUJFTF9SRVBFQVRFRBADImMKFE9uZW9mRGVzY3JpcHRvclBy
b3RvEhIKBG5hbWUYASABKAlSBG5hbWUSNwoHb3B0aW9ucxgCIAEoCzIdLmdvb2ds
ZS5wcm90b2J1Zi5PbmVvZk9wdGlvbnNSB29wdGlvbnMi4wIKE0VudW1EZXNjcmlw
dG9yUHJvdG8SEgoEbmFtZRgBIAEoCVIEbmFtZRI/CgV2YWx1ZRgCIAMoCzIpLmdv
b2dsZS5wcm90b2J1Zi5FbnVtVmFsdWVEZXNjcmlwdG9yUHJvdG9SBXZhbHVlEjYK
B29wdGlvbnMYAyABKAsyHC5nb29nbGUucHJvdG9idWYuRW51bU9wdGlvbnNSB29w
dGlvbnMSXQoOcmVzZXJ2ZWRfcmFuZ2UYBCADKAsyNi5nb29nbGUucHJvdG9idWYu
RW51bURlc2NyaXB0b3JQcm90by5FbnVtUmVzZXJ2ZWRSYW5nZVINcmVzZXJ2ZWRS
YW5nZRIjCg1yZXNlcnZlZF9uYW1lGAUgAygJUgxyZXNlcnZlZE5hbWUaOwoRRW51
bVJlc2VydmVkUmFuZ2USFAoFc3RhcnQYASABKAVSBXN0YXJ0EhAKA2VuZBgCIAEo
BVIDZW5kIoMBChhFbnVtVmFsdWVEZXNjcmlwdG9yUHJvdG8SEgoEbmFtZRgBIAEo
CVIEbmFtZRIWCgZudW1iZXIYAiABKAVSBm51bWJlchI7CgdvcHRpb25zGAMgASgL
MiEuZ29vZ2xlLnByb3RvYnVmLkVudW1WYWx1ZU9wdGlvbnNSB29wdGlvbnMipwEK
FlNlcnZpY2VEZXNjcmlwdG9yUHJvdG8SEgoEbmFtZRgBIAEoCVIEbmFtZRI+CgZt
ZXRob2QYAiADKAsyJi5nb29nbGUucHJvdG9idWYuTWV0aG9kRGVzY3JpcHRvclBy
b3RvUgZtZXRob2QSOQoHb3B0aW9ucxgDIAEoCzIfLmdvb2dsZS5wcm90b2J1Zi5T
ZXJ2aWNlT3B0aW9uc1IHb3B0aW9ucyKJAgoVTWV0aG9kRGVzY3JpcHRvclByb3Rv
EhIKBG5hbWUYASABKAlSBG5hbWUSHQoKaW5wdXRfdHlwZRgCIAEoCVIJaW5wdXRU
eXBlEh8KC291dHB1dF90eXBlGAMgASgJUgpvdXRwdXRUeXBlEjgKB29wdGlvbnMY
BCABKAsyHi5nb29nbGUucHJvdG9idWYuTWV0aG9kT3B0aW9uc1IHb3B0aW9ucxIw
ChBjbGllbnRfc3RyZWFtaW5nGAUgASgIOgVmYWxzZVIPY2xpZW50U3RyZWFtaW5n
EjAKEHNlcnZlcl9zdHJlYW1pbmcYBiABKAg6BWZhbHNlUg9zZXJ2ZXJTdHJlYW1p
bmcikgkKC0ZpbGVPcHRpb25zEiEKDGphdmFfcGFja2FnZRgBIAEoCVILamF2YVBh
Y2thZ2USMAoUamF2YV9vdXRlcl9jbGFzc25hbWUYCCABKAlSEmphdmFPdXRlckNs
YXNzbmFtZRI1ChNqYXZhX211bHRpcGxlX2ZpbGVzGAogASgIOgVmYWxzZVIRamF2
YU11bHRpcGxlRmlsZXMSRAodamF2YV9nZW5lcmF0ZV9lcXVhbHNfYW5kX2hhc2gY
FCABKAhCAhgBUhlqYXZhR2VuZXJhdGVFcXVhbHNBbmRIYXNoEjoKFmphdmFfc3Ry
aW5nX2NoZWNrX3V0ZjgYGyABKAg6BWZhbHNlUhNqYXZhU3RyaW5nQ2hlY2tVdGY4
ElMKDG9wdGltaXplX2ZvchgJIAEoDjIpLmdvb2dsZS5wcm90b2J1Zi5GaWxlT3B0
aW9ucy5PcHRpbWl6ZU1vZGU6BVNQRUVEUgtvcHRpbWl6ZUZvchIdCgpnb19wYWNr
YWdlGAsgASgJUglnb1BhY2thZ2USNQoTY2NfZ2VuZXJpY19zZXJ2aWNlcxgQIAEo
CDoFZmFsc2VSEWNjR2VuZXJpY1NlcnZpY2VzEjkKFWphdmFfZ2VuZXJpY19zZXJ2
aWNlcxgRIAEoCDoFZmFsc2VSE2phdmFHZW5lcmljU2VydmljZXMSNQoTcHlfZ2Vu
ZXJpY19zZXJ2aWNlcxgSIAEoCDoFZmFsc2VSEXB5R2VuZXJpY1NlcnZpY2VzEjcK
FHBocF9nZW5lcmljX3NlcnZpY2VzGCogASgIOgVmYWxzZVIScGhwR2VuZXJpY1Nl
cnZpY2VzEiUKCmRlcHJlY2F0ZWQYFyABKAg6BWZhbHNlUgpkZXByZWNhdGVkEi8K
EGNjX2VuYWJsZV9hcmVuYXMYHyABKAg6BWZhbHNlUg5jY0VuYWJsZUFyZW5hcxIq
ChFvYmpjX2NsYXNzX3ByZWZpeBgkIAEoCVIPb2JqY0NsYXNzUHJlZml4EikKEGNz
aGFycF9uYW1lc3BhY2UYJSABKAlSD2NzaGFycE5hbWVzcGFjZRIhCgxzd2lmdF9w
cmVmaXgYJyABKAlSC3N3aWZ0UHJlZml4EigKEHBocF9jbGFzc19wcmVmaXgYKCAB
KAlSDnBocENsYXNzUHJlZml4EiMKDXBocF9uYW1lc3BhY2UYKSABKAlSDHBocE5h
bWVzcGFjZRI0ChZwaHBfbWV0YWRhdGFfbmFtZXNwYWNlGCwgASgJUhRwaHBNZXRh
ZGF0YU5hbWVzcGFjZRIhCgxydWJ5X3BhY2thZ2UYLSABKAlSC3J1YnlQYWNrYWdl
ElgKFHVuaW50ZXJwcmV0ZWRfb3B0aW9uGOcHIAMoCzIkLmdvb2dsZS5wcm90b2J1
Zi5VbmludGVycHJldGVkT3B0aW9uUhN1bmludGVycHJldGVkT3B0aW9uIjoKDE9w
dGltaXplTW9kZRIJCgVTUEVFRBABEg0KCUNPREVfU0laRRACEhAKDExJVEVfUlVO
VElNRRADKgkI6AcQgICAgAJKBAgmECci0QIKDk1lc3NhZ2VPcHRpb25zEjwKF21l
c3NhZ2Vfc2V0X3dpcmVfZm9ybWF0GAEgASgIOgVmYWxzZVIUbWVzc2FnZVNldFdp
cmVGb3JtYXQSTAofbm9fc3RhbmRhcmRfZGVzY3JpcHRvcl9hY2Nlc3NvchgCIAEo
CDoFZmFsc2VSHG5vU3RhbmRhcmREZXNjcmlwdG9yQWNjZXNzb3ISJQoKZGVwcmVj
YXRlZBgDIAEoCDoFZmFsc2VSCmRlcHJlY2F0ZWQSGwoJbWFwX2VudHJ5GAcgASgI
UghtYXBFbnRyeRJYChR1bmludGVycHJldGVkX29wdGlvbhjnByADKAsyJC5nb29n
bGUucHJvdG9idWYuVW5pbnRlcnByZXRlZE9wdGlvblITdW5pbnRlcnByZXRlZE9w
dGlvbioJCOgHEICAgIACSgQICBAJSgQICRAKIuIDCgxGaWVsZE9wdGlvbnMSQQoF
Y3R5cGUYASABKA4yIy5nb29nbGUucHJvdG9idWYuRmllbGRPcHRpb25zLkNUeXBl
OgZTVFJJTkdSBWN0eXBlEhYKBnBhY2tlZBgCIAEoCFIGcGFja2VkEkcKBmpzdHlw
ZRgGIAEoDjIkLmdvb2dsZS5wcm90b2J1Zi5GaWVsZE9wdGlvbnMuSlNUeXBlOglK
U19OT1JNQUxSBmpzdHlwZRIZCgRsYXp5GAUgASgIOgVmYWxzZVIEbGF6eRIlCgpk
ZXByZWNhdGVkGAMgASgIOgVmYWxzZVIKZGVwcmVjYXRlZBIZCgR3ZWFrGAogASgI
OgVmYWxzZVIEd2VhaxJYChR1bmludGVycHJldGVkX29wdGlvbhjnByADKAsyJC5n
b29nbGUucHJvdG9idWYuVW5pbnRlcnByZXRlZE9wdGlvblITdW5pbnRlcnByZXRl
ZE9wdGlvbiIvCgVDVHlwZRIKCgZTVFJJTkcQABIICgRDT1JEEAESEAoMU1RSSU5H
X1BJRUNFEAIiNQoGSlNUeXBlEg0KCUpTX05PUk1BTBAAEg0KCUpTX1NUUklORxAB
Eg0KCUpTX05VTUJFUhACKgkI6AcQgICAgAJKBAgEEAUicwoMT25lb2ZPcHRpb25z
ElgKFHVuaW50ZXJwcmV0ZWRfb3B0aW9uGOcHIAMoCzIkLmdvb2dsZS5wcm90b2J1
Zi5VbmludGVycHJldGVkT3B0aW9uUhN1bmludGVycHJldGVkT3B0aW9uKgkI6AcQ
gICAgAIiwAEKC0VudW1PcHRpb25zEh8KC2FsbG93X2FsaWFzGAIgASgIUgphbGxv
d0FsaWFzEiUKCmRlcHJlY2F0ZWQYAyABKAg6BWZhbHNlUgpkZXByZWNhdGVkElgK
FHVuaW50ZXJwcmV0ZWRfb3B0aW9uGOcHIAMoCzIkLmdvb2dsZS5wcm90b2J1Zi5V
bmludGVycHJldGVkT3B0aW9uUhN1bmludGVycHJldGVkT3B0aW9uKgkI6AcQgICA
gAJKBAgFEAYingEKEEVudW1WYWx1ZU9wdGlvbnMSJQoKZGVwcmVjYXRlZBgBIAEo
CDoFZmFsc2VSCmRlcHJlY2F0ZWQSWAoUdW5pbnRlcnByZXRlZF9vcHRpb24Y5wcg
AygLMiQuZ29vZ2xlLnByb3RvYnVmLlVuaW50ZXJwcmV0ZWRPcHRpb25SE3VuaW50
ZXJwcmV0ZWRPcHRpb24qCQjoBxCAgICAAiKcAQoOU2VydmljZU9wdGlvbnMSJQoK
ZGVwcmVjYXRlZBghIAEoCDoFZmFsc2VSCmRlcHJlY2F0ZWQSWAoUdW5pbnRlcnBy
ZXRlZF9vcHRpb24Y5wcgAygLMiQuZ29vZ2xlLnByb3RvYnVmLlVuaW50ZXJwcmV0
ZWRPcHRpb25SE3VuaW50ZXJwcmV0ZWRPcHRpb24qCQjoBxCAgICAAiLgAgoNTWV0
aG9kT3B0aW9ucxIlCgpkZXByZWNhdGVkGCEgASgIOgVmYWxzZVIKZGVwcmVjYXRl
ZBJxChFpZGVtcG90ZW5jeV9sZXZlbBgiIAEoDjIvLmdvb2dsZS5wcm90b2J1Zi5N
ZXRob2RPcHRpb25zLklkZW1wb3RlbmN5TGV2ZWw6E0lERU1QT1RFTkNZX1VOS05P
V05SEGlkZW1wb3RlbmN5TGV2ZWwSWAoUdW5pbnRlcnByZXRlZF9vcHRpb24Y5wcg
AygLMiQuZ29vZ2xlLnByb3RvYnVmLlVuaW50ZXJwcmV0ZWRPcHRpb25SE3VuaW50
ZXJwcmV0ZWRPcHRpb24iUAoQSWRlbXBvdGVuY3lMZXZlbBIXChNJREVNUE9URU5D
WV9VTktOT1dOEAASEwoPTk9fU0lERV9FRkZFQ1RTEAESDgoKSURFTVBPVEVOVBAC
KgkI6AcQgICAgAIimgMKE1VuaW50ZXJwcmV0ZWRPcHRpb24SQQoEbmFtZRgCIAMo
CzItLmdvb2dsZS5wcm90b2J1Zi5VbmludGVycHJldGVkT3B0aW9uLk5hbWVQYXJ0
UgRuYW1lEikKEGlkZW50aWZpZXJfdmFsdWUYAyABKAlSD2lkZW50aWZpZXJWYWx1
ZRIsChJwb3NpdGl2ZV9pbnRfdmFsdWUYBCABKARSEHBvc2l0aXZlSW50VmFsdWUS
LAoSbmVnYXRpdmVfaW50X3ZhbHVlGAUgASgDUhBuZWdhdGl2ZUludFZhbHVlEiEK
DGRvdWJsZV92YWx1ZRgGIAEoAVILZG91YmxlVmFsdWUSIQoMc3RyaW5nX3ZhbHVl
GAcgASgMUgtzdHJpbmdWYWx1ZRInCg9hZ2dyZWdhdGVfdmFsdWUYCCABKAlSDmFn
Z3JlZ2F0ZVZhbHVlGkoKCE5hbWVQYXJ0EhsKCW5hbWVfcGFydBgBIAIoCVIIbmFt
ZVBhcnQSIQoMaXNfZXh0ZW5zaW9uGAIgAigIUgtpc0V4dGVuc2lvbiKnAgoOU291
cmNlQ29kZUluZm8SRAoIbG9jYXRpb24YASADKAsyKC5nb29nbGUucHJvdG9idWYu
U291cmNlQ29kZUluZm8uTG9jYXRpb25SCGxvY2F0aW9uGs4BCghMb2NhdGlvbhIW
CgRwYXRoGAEgAygFQgIQAVIEcGF0aBIWCgRzcGFuGAIgAygFQgIQAVIEc3BhbhIp
ChBsZWFkaW5nX2NvbW1lbnRzGAMgASgJUg9sZWFkaW5nQ29tbWVudHMSKwoRdHJh
aWxpbmdfY29tbWVudHMYBCABKAlSEHRyYWlsaW5nQ29tbWVudHMSOgoZbGVhZGlu
Z19kZXRhY2hlZF9jb21tZW50cxgGIAMoCVIXbGVhZGluZ0RldGFjaGVkQ29tbWVu
dHMi0QEKEUdlbmVyYXRlZENvZGVJbmZvEk0KCmFubm90YXRpb24YASADKAsyLS5n
b29nbGUucHJvdG9idWYuR2VuZXJhdGVkQ29kZUluZm8uQW5ub3RhdGlvblIKYW5u
b3RhdGlvbhptCgpBbm5vdGF0aW9uEhYKBHBhdGgYASADKAVCAhABUgRwYXRoEh8K
C3NvdXJjZV9maWxlGAIgASgJUgpzb3VyY2VGaWxlEhQKBWJlZ2luGAMgASgFUgVi
ZWdpbhIQCgNlbmQYBCABKAVSA2VuZEKPAQoTY29tLmdvb2dsZS5wcm90b2J1ZkIQ
RGVzY3JpcHRvclByb3Rvc0gBWj5naXRodWIuY29tL2dvbGFuZy9wcm90b2J1Zi9w
cm90b2MtZ2VuLWdvL2Rlc2NyaXB0b3I7ZGVzY3JpcHRvcvgBAaICA0dQQqoCGkdv
b2dsZS5Qcm90b2J1Zi5SZWZsZWN0aW9uCpkBChBlbnRpdHlfa2V5LnByb3RvEgpj
bG91ZHN0YXRlGiBnb29nbGUvcHJvdG9idWYvZGVzY3JpcHRvci5wcm90bzo+Cgpl
bnRpdHlfa2V5Eh0uZ29vZ2xlLnByb3RvYnVmLkZpZWxkT3B0aW9ucxjShgMgASgI
UgllbnRpdHlLZXlCDwoNaW8uY2xvdWRzdGF0ZWIGcHJvdG8zCtEICg5rdl9zdG9y
ZS5wcm90bxIKY2xvdWRzdGF0ZRoZZ29vZ2xlL3Byb3RvYnVmL2FueS5wcm90bxob
Z29vZ2xlL3Byb3RvYnVmL2VtcHR5LnByb3RvGhBlbnRpdHlfa2V5LnByb3RvInUK
E0RlbGV0ZVN0YXRlRW52ZWxvcGUSFgoDa2V5GAEgASgJQgSQtRgBUgNrZXkSMgoH
b3B0aW9ucxgCIAEoCzIYLmNsb3Vkc3RhdGUuU3RhdGVPcHRpb25zUgdvcHRpb25z
EhIKBGV0YWcYAyABKAlSBGV0YWcirAIKEVNhdmVTdGF0ZUVudmVsb3BlEhYKA2tl
eRgBIAEoCUIEkLUYAVIDa2V5EioKBXZhbHVlGAIgASgLMhQuZ29vZ2xlLnByb3Rv
YnVmLkFueVIFdmFsdWUSEgoEZXRhZxgDIAEoCVIEZXRhZxJHCghtZXRhZGF0YRgE
IAMoCzIrLmNsb3Vkc3RhdGUuU2F2ZVN0YXRlRW52ZWxvcGUuTWV0YWRhdGFFbnRy
eVIIbWV0YWRhdGESOQoHb3B0aW9ucxgFIAEoCzIfLmNsb3Vkc3RhdGUuU3RhdGVS
ZXF1ZXN0T3B0aW9uc1IHb3B0aW9ucxo7Cg1NZXRhZGF0YUVudHJ5EhAKA2tleRgB
IAEoCVIDa2V5EhQKBXZhbHVlGAIgASgJUgV2YWx1ZToCOAEiPgoQR2V0U3RhdGVF
bnZlbG9wZRIWCgNrZXkYASABKAlCBJC1GAFSA2tleRISCgRldGFnGAIgASgJUgRl
dGFnIlgKGEdldFN0YXRlUmVzcG9uc2VFbnZlbG9wZRIoCgRkYXRhGAEgASgLMhQu
Z29vZ2xlLnByb3RvYnVmLkFueVIEZGF0YRISCgRldGFnGAIgASgJUgRldGFnIlIK
DFN0YXRlT3B0aW9ucxIgCgtjb25jdXJyZW5jeRgBIAEoCVILY29uY3VycmVuY3kS
IAoLY29uc2lzdGVuY3kYAiABKAlSC2NvbnNpc3RlbmN5IlkKE1N0YXRlUmVxdWVz
dE9wdGlvbnMSIAoLY29uY3VycmVuY3kYASABKAlSC2NvbmN1cnJlbmN5EiAKC2Nv
bnNpc3RlbmN5GAIgASgJUgtjb25zaXN0ZW5jeTLxAQoNS2V5VmFsdWVTdG9yZRJQ
CghHZXRTdGF0ZRIcLmNsb3Vkc3RhdGUuR2V0U3RhdGVFbnZlbG9wZRokLmNsb3Vk
c3RhdGUuR2V0U3RhdGVSZXNwb25zZUVudmVsb3BlIgASRAoJU2F2ZVN0YXRlEh0u
Y2xvdWRzdGF0ZS5TYXZlU3RhdGVFbnZlbG9wZRoWLmdvb2dsZS5wcm90b2J1Zi5F
bXB0eSIAEkgKC0RlbGV0ZVN0YXRlEh8uY2xvdWRzdGF0ZS5EZWxldGVTdGF0ZUVu
dmVsb3BlGhYuZ29vZ2xlLnByb3RvYnVmLkVtcHR5IgBiBnByb3RvMw==`

type CRDT struct {
	connection *grpc.ClientConn
	metadata   *crdtMetadata
	json       jsoniter.API

	features []state.Feature
	logger   logger.Logger
}

type crdtMetadata struct {
	host       string
	serverPort int
}

func NewCRDT(logger logger.Logger) *CRDT {
	return &CRDT{
		json:     jsoniter.ConfigFastest,
		features: []state.Feature{state.FeatureETag},
		logger:   logger,
	}
}

// Init does metadata and connection parsing.
func (c *CRDT) Init(metadata state.Metadata) error {
	m, err := c.parseMetadata(metadata)
	if err != nil {
		return err
	}

	c.metadata = m
	go c.startServer()

	return nil
}

func (c *CRDT) Ping() error {
	return nil
}

// Features returns the features available in this state store.
func (c *CRDT) Features() []state.Feature {
	return c.features
}

func (c *CRDT) startServer() error {
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%v", c.metadata.serverPort))
	if err != nil {
		c.logger.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCrdtServer(s, c)
	pb.RegisterEntityDiscoveryServer(s, c)
	if err := s.Serve(lis); err != nil {
		c.logger.Fatalf("failed to serve: %v", err)
	}

	return nil
}

func (c *CRDT) Discover(ctx context.Context, in *pb.ProxyInfo) (*pb.EntitySpec, error) {
	d, err := base64.StdEncoding.DecodeString(fileDesc)
	if err != nil {
		return nil, err
	}

	entities := []*pb.Entity{
		{
			EntityType:    "cloudstate.crdt.Crdt",
			ServiceName:   "cloudstate.KeyValueStore",
			PersistenceId: "dapr",
		},
	}

	return &pb.EntitySpec{
		Proto:    d,
		Entities: entities,
	}, nil
}

func (c *CRDT) ReportError(ctx context.Context, in *pb.UserFunctionError) (*empty.Empty, error) {
	c.logger.Errorf("error from CloudState: %s", in.GetMessage())

	return &empty.Empty{}, nil
}

func (c *CRDT) Handle(srv pb.Crdt_HandleServer) error {
	var val *any.Any

	exists := false

	for {
		strmIn, err := srv.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			c.logger.Debugf("error from src.Recv(): %s", err)
			srv.Context().Done()

			return err
		}

		msg := strmIn.GetMessage()

		//nolint:govet
		switch m := msg.(type) {
		case *pb.CrdtStreamIn_Init:
			if m.Init.State != nil {
				r := m.Init.State.GetLwwregister()
				val = r.Value
				exists = true
			}
		case *pb.CrdtStreamIn_Changed:
			r := m.Changed.GetLwwregister()
			val = r.Value
		case *pb.CrdtStreamIn_Command:
			switch m.Command.Name {
			case "GetState":
				resp := kvstore_pb.GetStateResponseEnvelope{
					Data: val,
				}
				a, _ := ptypes.MarshalAny(&resp)

				srv.Send(&pb.CrdtStreamOut{
					Message: &pb.CrdtStreamOut_Reply{
						Reply: &pb.CrdtReply{
							CommandId: m.Command.Id,
							ClientAction: &pb.ClientAction{
								Action: &pb.ClientAction_Reply{
									Reply: &pb.Reply{
										Payload: a,
									},
								},
							},
						},
					},
				})
			case "DeleteState":
				val = &any.Any{}
				e := empty.Empty{}
				a, _ := ptypes.MarshalAny(&e)
				srv.Send(&pb.CrdtStreamOut{
					Message: &pb.CrdtStreamOut_Reply{
						Reply: &pb.CrdtReply{
							CommandId: m.Command.Id,
							ClientAction: &pb.ClientAction{
								Action: &pb.ClientAction_Reply{
									Reply: &pb.Reply{
										Payload: a,
									},
								},
							},
							StateAction: &pb.CrdtStateAction{
								Action: &pb.CrdtStateAction_Update{
									&pb.CrdtDelta{
										Delta: &pb.CrdtDelta_Lwwregister{
											Lwwregister: &pb.LWWRegisterDelta{
												Value: val,
											},
										},
									},
								},
							},
						},
					},
				})
			case "SaveState":
				e := empty.Empty{}
				a, _ := ptypes.MarshalAny(&e)

				if m.Command.GetPayload().Value != nil {
					vAny := m.Command.GetPayload()
					var saveState kvstore_pb.SaveStateEnvelope
					err := ptypes.UnmarshalAny(vAny, &saveState)
					if err != nil {
						c.logger.Error(err)

						break
					}

					val = saveState.GetValue()

					var act *pb.CrdtStateAction
					if exists {
						act = &pb.CrdtStateAction{
							Action: &pb.CrdtStateAction_Update{
								&pb.CrdtDelta{
									Delta: &pb.CrdtDelta_Lwwregister{
										Lwwregister: &pb.LWWRegisterDelta{
											Value: val,
										},
									},
								},
							},
						}
					} else {
						act = &pb.CrdtStateAction{
							Action: &pb.CrdtStateAction_Create{
								&pb.CrdtState{
									State: &pb.CrdtState_Lwwregister{
										Lwwregister: &pb.LWWRegisterState{
											Value: val,
										},
									},
								},
							},
						}
					}

					srv.Send(&pb.CrdtStreamOut{
						Message: &pb.CrdtStreamOut_Reply{
							Reply: &pb.CrdtReply{
								CommandId: m.Command.Id,
								ClientAction: &pb.ClientAction{
									Action: &pb.ClientAction_Reply{
										Reply: &pb.Reply{
											Payload: a,
										},
									},
								},
								StateAction: act,
							},
						},
					})
				}
			}
		}
	}

	return nil
}

// Since CloudState runs as a sidecar, we're pushing the connection init to be lazily executed when a request comes in to
// Give CloudState ample time to start and form a cluster.
func (c *CRDT) createConnectionOnce() error {
	var connError error
	doOnce.Do(func() {
		conn, err := grpc.Dial(c.metadata.host, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			connError = fmt.Errorf("couldn't establish connection to CloudState: %s", err)
		} else {
			c.connection = conn
		}
	})

	return connError
}

func (c *CRDT) parseMetadata(metadata state.Metadata) (*crdtMetadata, error) {
	m := crdtMetadata{}
	if val, ok := metadata.Properties[host]; ok && val != "" {
		m.host = val
	} else {
		return nil, fmt.Errorf("host field required")
	}

	if val, ok := metadata.Properties[serverPort]; ok && val != "" {
		port, err := strconv.Atoi(val)
		if err != nil {
			return nil, err
		}
		m.serverPort = port
	} else {
		return nil, fmt.Errorf("serverPort field required")
	}

	return &m, nil
}

func (c *CRDT) getClient() kvstore_pb.KeyValueStoreClient {
	return kvstore_pb.NewKeyValueStoreClient(c.connection)
}

// Get retrieves state from CloudState with a key.
func (c *CRDT) Get(req *state.GetRequest) (*state.GetResponse, error) {
	err := c.createConnectionOnce()
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	client := c.getClient()
	resp, err := client.GetState(ctx, &kvstore_pb.GetStateEnvelope{
		Key: req.Key,
	})
	if err != nil {
		return nil, err
	}

	stateResp := &state.GetResponse{}
	if resp.Data != nil {
		stateResp.Data = resp.Data.Value
	}

	return stateResp, nil
}

// BulkGet performs a bulks get operations.
func (c *CRDT) BulkGet(req []state.GetRequest) (bool, []state.BulkGetResponse, error) {
	return false, nil, nil
}

// Delete performs a delete operation.
func (c *CRDT) Delete(req *state.DeleteRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	client := c.getClient()

	var etag string
	if req.ETag != nil {
		etag = *req.ETag
	}
	_, err = client.DeleteState(ctx, &kvstore_pb.DeleteStateEnvelope{
		Key:  req.Key,
		Etag: etag,
	})

	return err
}

// BulkDelete performs a bulk delete operation.
func (c *CRDT) BulkDelete(req []state.DeleteRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	for i := range req {
		err = c.Delete(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}

// Set saves state into CloudState.
func (c *CRDT) Set(req *state.SetRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), defaultOperationTimeout)
	defer cancel()

	var bt []byte
	b, ok := req.Value.([]byte)
	if ok {
		bt = b
	} else {
		bt, _ = c.json.Marshal(req.Value)
	}

	client := c.getClient()
	var etag string
	if req.ETag != nil {
		etag = *req.ETag
	}

	_, err = client.SaveState(ctx, &kvstore_pb.SaveStateEnvelope{
		Key:  req.Key,
		Etag: etag,
		Value: &any.Any{
			Value: bt,
		},
	})

	return err
}

// BulkSet performs a bulks save operation.
func (c *CRDT) BulkSet(req []state.SetRequest) error {
	err := c.createConnectionOnce()
	if err != nil {
		return err
	}

	for i := range req {
		err = c.Set(&req[i])
		if err != nil {
			return err
		}
	}

	return nil
}
