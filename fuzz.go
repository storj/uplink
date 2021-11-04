package uplink    // rename if needed

func Fuzz_Access_OverrideEncryptionKey(access *uplink.Access, bucket string, prefix string, encryptionKey *uplink.EncryptionKey) {
	if access == nil {
		return
	}
	if encryptionKey == nil {
		return
	}
	access.OverrideEncryptionKey(bucket, prefix, encryptionKey)
}

func Fuzz_Access_SatelliteAddress(access *uplink.Access) {
	if access == nil {
		return
	}
	access.SatelliteAddress()
}

func Fuzz_Access_Serialize(access *uplink.Access) {
	if access == nil {
		return
	}
	access.Serialize()
}

func Fuzz_Access_Share(access *uplink.Access, permission uplink.Permission, prefixes []uplink.SharePrefix) {
	if access == nil {
		return
	}
	access.Share(permission, prefixes...)
}

func Fuzz_BucketIterator_Err(buckets *uplink.BucketIterator) {
	if buckets == nil {
		return
	}
	buckets.Err()
}

func Fuzz_BucketIterator_Item(buckets *uplink.BucketIterator) {
	if buckets == nil {
		return
	}
	buckets.Item()
}

func Fuzz_BucketIterator_Next(buckets *uplink.BucketIterator) {
	if buckets == nil {
		return
	}
	buckets.Next()
}

func Fuzz_Download_Close(download *uplink.Download) {
	if download == nil {
		return
	}
	download.Close()
}

func Fuzz_Download_Info(download *uplink.Download) {
	if download == nil {
		return
	}
	download.Info()
}

func Fuzz_Download_Read(download *uplink.Download, p []byte) {
	if download == nil {
		return
	}
	download.Read(p)
}

func Fuzz_ObjectIterator_Err(objects *uplink.ObjectIterator) {
	if objects == nil {
		return
	}
	objects.Err()
}

func Fuzz_ObjectIterator_Item(objects *uplink.ObjectIterator) {
	if objects == nil {
		return
	}
	objects.Item()
}

func Fuzz_ObjectIterator_Next(objects *uplink.ObjectIterator) {
	if objects == nil {
		return
	}
	objects.Next()
}

func Fuzz_PartIterator_Err(parts *uplink.PartIterator) {
	if parts == nil {
		return
	}
	parts.Err()
}

func Fuzz_PartIterator_Item(parts *uplink.PartIterator) {
	if parts == nil {
		return
	}
	parts.Item()
}

func Fuzz_PartIterator_Next(parts *uplink.PartIterator) {
	if parts == nil {
		return
	}
	parts.Next()
}

