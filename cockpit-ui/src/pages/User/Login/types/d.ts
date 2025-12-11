// types/apiResponse.ts
export interface ApiResponse<T> {
    code: number;
    msg: string;
    data: T;
}
