package uplink

//import "context"

//func Fuzz(data []byte) int {
//	ctx := context.Background()
//	_, _ = RequestAccessWithPassphrase(ctx, string(data), string(data), string(data))
//	return 1
//}
//func Fuzz_DeriveEncryptionKey(data []byte) int {
//	_,_ = DeriveEncryptionKey(string(data), data)
//	return 1
//}

//func Fuzz(data []byte) int {
//	//ctx := context.Background()
//	_, _ = ParseAccess(string(data))
//	return 1
//}
//func Fuzz(data []byte) int {
//	ctx := context.Background()
//	_, _ = RequestAccessWithPassphrase(ctx, string(data), string(data), string(data) )
//	return 1
//}

func Fuzz_DeriveEncryptionKey(passphrase string, salt []byte) {
	_, _ =DeriveEncryptionKey(passphrase, salt)
}





