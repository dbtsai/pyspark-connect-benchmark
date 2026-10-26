"""Validate upload framing, lengths and CRCs without retaining artifact contents."""
import zlib


def receive(requests, proto, server_id, count, remember):
    response = proto.AddArtifactsResponse(server_side_session_id=server_id)
    pending = None

    def chunk_bytes(chunk):
        assert zlib.crc32(chunk.data) == chunk.crc, 'Artifact CRC mismatch'
        count(artifact_chunks=1, artifact_payload_bytes=len(chunk.data))
        return len(chunk.data)

    def complete(name):
        response.artifacts.add(name=name, is_crc_successful=True)
        remember(response.session_id, name)
        count(artifact_files=1)

    def finish():
        nonlocal pending
        if pending is not None:
            name, total, num, received, chunks = pending
            assert (received, chunks) == (total, num), 'Artifact length/chunk count mismatch'
            complete(name)
            pending = None

    for request in requests:
        if response.session_id:
            assert request.session_id == response.session_id, 'Artifact session changed'
        response.session_id = request.session_id
        count(rpc_request_bytes=request.ByteSize())
        if request.HasField('batch'):
            finish()
            for artifact in request.batch.artifacts:
                chunk_bytes(artifact.data)
                complete(artifact.name)
        elif request.HasField('begin_chunk'):
            finish()
            b = request.begin_chunk
            pending = [b.name, b.total_bytes, b.num_chunks, chunk_bytes(b.initial_chunk), 1]
        elif request.HasField('chunk'):
            assert pending is not None, 'Chunk without begin_chunk'
            pending[3] += chunk_bytes(request.chunk)
            pending[4] += 1
        else:
            raise AssertionError('Unscripted artifact request')
    finish()
    return response
